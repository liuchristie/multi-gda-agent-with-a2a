import contextlib
import json
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Optional, Union

import google.auth
import google.auth.transport.requests
import httpx
from a2a.client import ClientCallContext
from a2a.client.card_resolver import parse_agent_card
from a2a.client.client import ClientConfig as A2AClientConfig
from a2a.client.errors import A2AClientError
from a2a.client.transports import http_helpers
from a2a.compat.v0_3 import rest_transport
from a2a.server.events import Event as A2AEvent
from a2a.types import (
    AgentCard,
    Message as A2AMessage,
    Part as A2APart,
    Role,
    Task,
    TaskArtifactUpdateEvent,
    TaskStatusUpdateEvent,
)
from a2a.utils.constants import TransportProtocol
from google.adk.a2a import _compat
from google.adk.a2a.agent import (
    A2aRemoteAgentConfig,
    ParametersConfig,
    RequestInterceptor,
)
from google.adk.a2a.converters.part_converter import (
    A2A_DATA_PART_METADATA_TYPE_CODE_EXECUTION_RESULT,
    A2A_DATA_PART_METADATA_TYPE_EXECUTABLE_CODE,
    A2A_DATA_PART_METADATA_TYPE_FUNCTION_CALL,
    A2A_DATA_PART_METADATA_TYPE_FUNCTION_RESPONSE,
    A2A_DATA_PART_METADATA_TYPE_KEY,
    A2APartToGenAIPartConverter,
    convert_a2a_part_to_genai_part,
)
from google.adk.a2a.converters.to_adk_event import convert_a2a_task_to_event
from google.adk.a2a.converters.utils import _get_adk_metadata_key
from google.adk.agents.invocation_context import InvocationContext
from google.adk.agents.remote_a2a_agent import RemoteA2aAgent
from google.adk.events.event import Event as AdkEvent
from google.adk.events.event_actions import EventActions
from google.api_core import client_options
from google.cloud import geminidataanalytics
from google.genai import types as genai_types
from google.protobuf import json_format
from pydantic import Field

from app.config import (
    AGENTS_BILLING_PROJECT,
    AGENTS_FILTER,
    AGENTS_LOCATION,
    GDA_AGENTS_BILLING_PROJECT,
    GDA_AGENTS_LOCATION,
)
from app.render_chart import apply_google_theme, extract_vega_config, render_chart_markdown

logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# A2A Compatibility Patches for Google Cloud REST A2A Endpoints
# -----------------------------------------------------------------------------
# 1. Google Cloud REST streaming endpoints return SSE chunks as a JSON array
#    `[{...}, {...}]` rather than naked JSON objects. A2A parser expects single
#    JSON objects per SSE event. We unpack any array chunks into individual events.
_orig_send_http_stream_request = http_helpers.send_http_stream_request


async def _gda_unwrapped_send_http_stream_request(*args, **kwargs):
    async for sse_data in _orig_send_http_stream_request(*args, **kwargs):
        trimmed = sse_data.strip()
        if trimmed.startswith("[") and trimmed.endswith("]"):
            try:
                items = json.loads(trimmed)
                if isinstance(items, list):
                    for item in items:
                        yield json.dumps(item)
                    continue
            except Exception:
                pass
        yield sse_data


http_helpers.send_http_stream_request = _gda_unwrapped_send_http_stream_request
rest_transport.send_http_stream_request = _gda_unwrapped_send_http_stream_request
_orig_handle_http_error = rest_transport.CompatRestTransport._handle_http_error


def _gda_handle_http_error(self, e: httpx.HTTPStatusError):
    try:
        with contextlib.suppress(httpx.StreamClosed):
            e.response.read()
        try:
            error_data = e.response.json()
        except Exception:
            error_data = {}

        if isinstance(error_data, list) and error_data:
            error_data = error_data[0]
        if (
            isinstance(error_data, dict)
            and "error" in error_data
            and isinstance(error_data["error"], dict)
        ):
            error_data = error_data["error"]
        if not isinstance(error_data, dict):
            error_data = {}

        message = error_data.get("message") or str(e)
        status_code = e.response.status_code
        raise A2AClientError(f"HTTP Error {status_code}: {message}") from e
    except A2AClientError:
        raise
    except Exception:
        _orig_handle_http_error(self, e)


rest_transport.CompatRestTransport._handle_http_error = _gda_handle_http_error

EXT_ADK_A2A = "https://google.github.io/adk-docs/a2a/a2a-extension/"
EXT_SSE = "https://www.googleapis.com/gemini-enterprise/a2a/extensions/sse/v1"
EXT_A2UI_THOUGHTS = (
    "https://www.googleapis.com/gemini-enterprise/a2a/extensions/a2ui_thoughts/v1"
)
EXT_SDUI_CHART = "https://cloud.google.com/bigquery/agent/extensions/sdui-chart/v1"


class GoogleAdcAuth(httpx.Auth):
    """HTTPX authentication handler that automatically refreshes Google Cloud ADC tokens."""

    def __init__(self, scopes: list[str] | None = None):
        self.scopes = scopes or ["https://www.googleapis.com/auth/cloud-platform"]
        self.creds, _ = google.auth.default(scopes=self.scopes)
        self.auth_request = google.auth.transport.requests.Request()
        self._lock = threading.Lock()
        self.refresh_token()

    def refresh_token(self) -> None:
        with self._lock:
            if not self.creds.valid:
                self.creds.refresh(self.auth_request)

    def auth_flow(self, request):
        if not self.creds.valid:
            self.refresh_token()
        request.headers["Authorization"] = f"Bearer {self.creds.token}"
        yield request


a2a_client_config = A2AClientConfig(
    streaming=True,
    supported_protocol_bindings=[
        TransportProtocol.HTTP_JSON,
        TransportProtocol.JSONRPC,
    ],
)

_CARD_CACHE: dict[str, AgentCard] = {}
_DEFAULT_AUTH: Optional[GoogleAdcAuth] = None
_AUTH_LOCK = threading.Lock()


def get_default_auth() -> GoogleAdcAuth:
    """Thread-safe singleton for Google ADC authentication."""
    global _DEFAULT_AUTH
    if _DEFAULT_AUTH is None:
        with _AUTH_LOCK:
            if _DEFAULT_AUTH is None:
                _DEFAULT_AUTH = GoogleAdcAuth()
    return _DEFAULT_AUTH


class GDARemoteAgent(RemoteA2aAgent):
    agent_resource_name: str = ""
    history_length: Optional[int] = 20
    base_url: str = "https://geminidataanalytics.googleapis.com/v1/a2a"
    extensions: list[str] = Field(
        default_factory=lambda: [
            EXT_ADK_A2A,
            EXT_SSE,
            EXT_A2UI_THOUGHTS,
            EXT_SDUI_CHART,
        ]
    )
    google_auth: Any = None

    def __init__(
        self,
        agent_resource_name: str,
        description: str = "",
        agent_card: Optional[AgentCard] = None,
        a2a_history_length: Optional[int] = 20,
        google_auth: Optional[httpx.Auth] = None,
        **kwargs: Any,
    ):
        raw_name = agent_resource_name.split("/")[-1]
        name = raw_name.replace("-", "_")
        auth = google_auth or get_default_auth()
        card = agent_card or self._fetch_card(agent_resource_name, auth)

        httpx_client = kwargs.pop("httpx_client", None)
        if httpx_client is None:
            httpx_client = httpx.AsyncClient(
                auth=auth,
                timeout=httpx.Timeout(120.0),
            )

        interceptor = RequestInterceptor(
            before_request=self.outbound_request_interceptor
        )
        config = A2aRemoteAgentConfig(
            a2a_task_converter=self.inbound_custom_task_converter,
            a2a_status_update_converter=self.custom_status_parser,
            a2a_artifact_update_converter=self.custom_artifact_parser,
            a2a_part_converter=self.custom_a2a_part_converter,
            request_interceptors=[interceptor],
        )

        super().__init__(
            name=name,
            agent_card=card,
            description=description or getattr(card, "description", ""),
            config=config,
            a2a_part_converter=self.custom_a2a_part_converter,
            agent_resource_name=agent_resource_name,
            history_length=a2a_history_length,
            google_auth=auth,
            httpx_client=httpx_client,
            **kwargs,
        )

    async def _handle_a2a_response(
        self,
        a2a_response: Any,
        ctx: InvocationContext,
    ) -> Optional[AdkEvent]:
        """Always route responses to v2 handler so A2aRemoteAgentConfig converters are used."""
        event = await self._handle_a2a_response_v2(a2a_response, ctx)
        if event:
            context_id = None
            if isinstance(a2a_response, tuple) and a2a_response[0]:
                context_id = getattr(a2a_response[0], "context_id", None)
            elif hasattr(a2a_response, "context_id"):
                context_id = getattr(a2a_response, "context_id", None)

            if context_id:
                if ctx and ctx.session:
                    ctx.session.state[f"gda_context_{self.name}"] = context_id
                event.actions = event.actions or EventActions()
                event.actions.state_delta[f"gda_context_{self.name}"] = context_id
        return event

    @staticmethod
    def custom_a2a_part_converter(
        a2a_part: A2APart,
    ) -> Optional[genai_types.Part]:
        """Convert an A2A Part to a GenAI Part, cleanly suppressing untyped/internal data parts."""
        if _compat.is_data_part(a2a_part):
            meta = _compat.part_metadata(a2a_part)
            meta_key = _get_adk_metadata_key(A2A_DATA_PART_METADATA_TYPE_KEY)
            part_type = meta.get(meta_key) if meta else None

            # Only preserve data parts that map to supported GenAI execution types
            if part_type in (
                A2A_DATA_PART_METADATA_TYPE_FUNCTION_CALL,
                A2A_DATA_PART_METADATA_TYPE_FUNCTION_RESPONSE,
                A2A_DATA_PART_METADATA_TYPE_CODE_EXECUTION_RESULT,
                A2A_DATA_PART_METADATA_TYPE_EXECUTABLE_CODE,
            ):
                return convert_a2a_part_to_genai_part(a2a_part)

            # Suppress generic / empty data parts (e.g. BigQuery job metadata, schema {}, internal stats)
            # so ADK does not convert them into <a2a_datapart_json> text blobs in the user response.
            return None

        return convert_a2a_part_to_genai_part(a2a_part)

    @staticmethod
    def _fetch_card(
        agent_resource_name: str,
        auth: httpx.Auth,
        client: Optional[httpx.Client] = None,
    ) -> AgentCard:
        if agent_resource_name in _CARD_CACHE:
            return _CARD_CACHE[agent_resource_name]

        base_url = "https://geminidataanalytics.googleapis.com/v1/a2a"
        agent_card_path = f"{base_url}/{agent_resource_name}/v1/card"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if client is not None:
            response = client.get(agent_card_path, headers=headers)
            response.raise_for_status()
            card = parse_agent_card(response.json())
            _CARD_CACHE[agent_resource_name] = card
            return card

        with httpx.Client(
            auth=auth,
            timeout=None,
            headers=headers,
        ) as c:
            response = c.get(agent_card_path)
            response.raise_for_status()
            card = parse_agent_card(response.json())
            _CARD_CACHE[agent_resource_name] = card
            return card

    def inbound_custom_task_converter(
        self,
        a2a_task: Task,
        agent_name: str | None,
        ctx: InvocationContext | None,
        part_converter: A2APartToGenAIPartConverter,
    ) -> AdkEvent | None:
        if a2a_task.context_id and ctx and ctx.session:
            ctx.session.state[f"gda_context_{self.name}"] = a2a_task.context_id
        if (
            a2a_task.status
            and a2a_task.status.state in (_compat.TS_WORKING, _compat.TS_SUBMITTED)
            and not a2a_task.artifacts
        ):
            return None
        return convert_a2a_task_to_event(a2a_task, agent_name, ctx, part_converter)

    async def outbound_request_interceptor(
        self,
        ctx: InvocationContext,
        a2a_request: A2AMessage,
        params: ParametersConfig,
    ) -> tuple[Union[A2AMessage, AdkEvent], ParametersConfig]:
        if params.client_call_context is None:
            params.client_call_context = ClientCallContext()

        params.client_call_context.timeout = 120.0
        if params.client_call_context.service_parameters is None:
            params.client_call_context.service_parameters = {}

        params.client_call_context.service_parameters.update({
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "X-A2A-Extensions": ",".join(self.extensions),
        })

        if isinstance(a2a_request, dict):
            a2a_request["role"] = "ROLE_USER"
            if ctx and ctx.session and ctx.session.state:
                if gda_context := ctx.session.state.get(f"gda_context_{self.name}"):
                    a2a_request["context_id"] = gda_context
        else:
            try:
                a2a_request.role = Role.ROLE_USER
            except (AttributeError, ValueError):
                pass
            if ctx and ctx.session and ctx.session.state:
                if gda_context := ctx.session.state.get(f"gda_context_{self.name}"):
                    a2a_request.context_id = gda_context

        return a2a_request, params

    @staticmethod
    def _extract_followup_questions(item: dict[str, Any]) -> list[str]:
        """Extract follow-up questions from a status update content item."""
        data_block = item.get("data")
        if isinstance(data_block, str):
            try:
                data_block = json.loads(data_block)
            except Exception:
                data_block = {}

        candidates: list[dict[str, Any]] = []
        if isinstance(data_block, dict):
            candidates.append(data_block)
            inner = data_block.get("data")
            if isinstance(inner, str):
                try:
                    inner = json.loads(inner)
                except Exception:
                    pass
            if isinstance(inner, dict):
                candidates.append(inner)
        candidates.append(item)

        questions_raw: Any = None
        for cand in candidates:
            if not isinstance(cand, dict):
                continue
            resp = (
                cand.get("recommendedQuestionsResponse")
                or cand.get("recommended_questions_response")
                or cand.get("recommendedQuestions")
                or cand.get("recommended_questions")
            )
            if isinstance(resp, str):
                try:
                    resp = json.loads(resp)
                except Exception:
                    pass
            if isinstance(resp, dict):
                q_list = resp.get("questions") or resp.get("question_list")
                if isinstance(q_list, list):
                    questions_raw = q_list
                    break
            elif isinstance(resp, list):
                questions_raw = resp
                break
            if "questions" in cand and isinstance(cand["questions"], list):
                questions_raw = cand["questions"]
                break

        if not questions_raw:
            metadata = item.get("metadata")
            if isinstance(metadata, dict):
                gda_msg = metadata.get("gda_message", {})
                if isinstance(gda_msg, dict) and gda_msg.get("subType") == "followup_questions":
                    for cand in candidates:
                        if isinstance(cand, dict) and isinstance(cand.get("questions"), list):
                            questions_raw = cand["questions"]
                            break

        if not questions_raw:
            return []

        cleaned: list[str] = []
        for q in questions_raw:
            if isinstance(q, str):
                q_clean = q.strip().strip('"').strip("'").strip()
                if q_clean:
                    cleaned.append(q_clean)
            elif isinstance(q, dict):
                text_val = q.get("text") or q.get("question") or ""
                if isinstance(text_val, str):
                    text_val = text_val.strip().strip('"').strip("'").strip()
                    if text_val:
                        cleaned.append(text_val)
        return cleaned

    def custom_status_parser(
        self,
        update: TaskStatusUpdateEvent,
        agent_name: str | None,
        ctx: InvocationContext | None,
        part_converter: A2APartToGenAIPartConverter,
    ) -> AdkEvent | None:
        if isinstance(update, dict):
            status_obj = update.get("status") or update.get("statusUpdate", {}).get("status")
            message = status_obj.get("message") if isinstance(status_obj, dict) else getattr(status_obj, "message", None)
        else:
            message = update.status.message if hasattr(update, "status") and update.status else None

        if not message:
            return None

        if hasattr(message, "DESCRIPTOR"):
            message_dict = json_format.MessageToDict(
                message, preserving_proto_field_name=True
            )
        elif isinstance(message, dict):
            message_dict = message
        else:
            message_dict = getattr(message, "__dict__", {})

        if not isinstance(message_dict, dict):
            return None

        # Capture context_id from status update into session state if available
        context_id = getattr(update, "context_id", None)
        if not context_id and isinstance(update, dict):
            context_id = (
                update.get("context_id")
                or update.get("contextId")
                or update.get("statusUpdate", {}).get("context_id")
                or update.get("statusUpdate", {}).get("contextId")
            )
        if not context_id and isinstance(message_dict, dict):
            context_id = message_dict.get("context_id") or message_dict.get("contextId")
        if context_id and ctx and ctx.session:
            ctx.session.state[f"gda_context_{self.name}"] = context_id

        content_list = message_dict.get("parts") or message_dict.get("content") or []
        if not isinstance(content_list, list) or not content_list:
            return None

        adk_parts = []
        for item in content_list:
            if not isinstance(item, dict):
                continue

            # Check for follow-up questions
            questions = self._extract_followup_questions(item)
            if questions:
                formatted_questions = "\n".join(f"* {q}" for q in questions)
                followup_text = (
                    f"**Recommended Follow Up Questions:**\n{formatted_questions}"
                )
                adk_parts.append(genai_types.Part.from_text(text=followup_text))
                continue

            text = item.get("text", "")
            metadata = item.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            gda_message = metadata.get("gda_message", {})
            if not isinstance(gda_message, dict):
                gda_message = {}
            is_thought = gda_message.get("subType") == "thought"

            if not text and not is_thought:
                continue
            adk_part = genai_types.Part.from_text(text=text)
            if is_thought and hasattr(adk_part, "thought"):
                adk_part.thought = True
            adk_parts.append(adk_part)

        if not adk_parts:
            return None

        event = AdkEvent(
            author=agent_name or self.name,
            content=genai_types.Content(role="agent", parts=adk_parts),
            invocation_id=ctx.invocation_id if ctx else None,
            branch=ctx.branch if ctx else None,
            partial=True,
        )
        if context_id:
            event.actions = event.actions or EventActions()
            event.actions.state_delta[f"gda_context_{self.name}"] = context_id
        return event

    def custom_artifact_parser(
        self,
        update: TaskArtifactUpdateEvent,
        agent_name: str | None,
        ctx: InvocationContext | None,
        part_converter: A2APartToGenAIPartConverter,
    ) -> AdkEvent | None:
        if hasattr(update, "DESCRIPTOR"):
            update_dict = json_format.MessageToDict(
                update, preserving_proto_field_name=True
            )
        elif isinstance(update, dict):
            update_dict = update
        else:
            update_dict = getattr(update, "__dict__", {})

        if not isinstance(update_dict, dict):
            return None

        artifact = update_dict.get("artifact", {})
        if not isinstance(artifact, dict):
            return None
        parts = artifact.get("parts", [])
        if not isinstance(parts, list) or not parts:
            return None

        artifact_name = artifact.get("name", "")
        adk_parts = []
        for a2a_part in parts:
            if not isinstance(a2a_part, dict):
                continue
            metadata = a2a_part.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            text = a2a_part.get("text", "")
            data_block = a2a_part.get("data", {})
            gda_message = metadata.get("gda_message", {})
            if not isinstance(gda_message, dict):
                gda_message = {}
            sub_type = gda_message.get("subType")

            if artifact_name == "Data result":
                if sub_type == "result_name":
                    if gda_message.get("value") == "verified_query":
                        adk_parts.append(
                            genai_types.Part.from_text(text="**Verified Query**")
                        )
                elif sub_type == "result_data":
                    if text:
                        adk_parts.append(genai_types.Part.from_text(text=text))

            elif artifact_name == "Final response":
                if sub_type == "final_response" and text:
                    adk_parts.append(genai_types.Part.from_text(text=text))

            elif artifact_name == "Generated SQL":
                if sub_type == "generated_sql":
                    sql_query = gda_message.get("value")
                    if sql_query:
                        formatted_sql = f"```sql\n{sql_query}\n```"
                        adk_parts.append(
                            genai_types.Part.from_text(text=formatted_sql)
                        )

            elif artifact_name == "Chart result":
                if sub_type == "result_vega_config":
                    vega_config = (
                        extract_vega_config(data_block)
                        or (data_block.get("data") if isinstance(data_block, dict) else None)
                        or data_block
                    )
                    if vega_config and isinstance(vega_config, dict):
                        google_vega_config = apply_google_theme(vega_config)
                        chart = render_chart_markdown(google_vega_config)
                        if chart:
                            adk_parts.append(genai_types.Part.from_text(text=chart))

        if not adk_parts:
            return None

        last_chunk = update_dict.get("last_chunk", False)
        return AdkEvent(
            author=agent_name or self.name,
            content=genai_types.Content(role="agent", parts=adk_parts),
            invocation_id=ctx.invocation_id if ctx else None,
            branch=ctx.branch if ctx else None,
            partial=not last_chunk,
        )


def list_gda_agents(
    project: str = AGENTS_BILLING_PROJECT,
    location: str = AGENTS_LOCATION,
    agents_filter: str = AGENTS_FILTER,
) -> list[dict[str, Any]]:
    client = geminidataanalytics.DataAgentServiceClient(
        client_options=client_options.ClientOptions(
            api_endpoint="geminidataanalytics.googleapis.com"
        )
    )

    try:
        req = geminidataanalytics.ListAccessibleDataAgentsRequest(
            parent=f"projects/{project}/locations/{location}",
            creator_filter=geminidataanalytics.ListAccessibleDataAgentsRequest.CreatorFilter.NONE,
            filter=agents_filter,
        )
        response = client.list_accessible_data_agents(request=req)
        agents = [
            {
                "agent_resource_name": r.name,
                "description": r.description,
            }
            for r in response
        ]
        return agents
    except Exception as e:
        logger.error("Failed to list GDA agents: %s", e)
        return []


def get_remote_agent() -> list[RemoteA2aAgent]:
    """Discovers and initializes all remote GDA agents."""
    agents = list_gda_agents(
        agents_filter=AGENTS_FILTER,
        project=AGENTS_BILLING_PROJECT,
        location=AGENTS_LOCATION,
    )
    if not agents:
        return []

    auth = get_default_auth()

    subagents: list[RemoteA2aAgent] = []
    with httpx.Client(
        auth=auth,
        timeout=httpx.Timeout(30.0),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    ) as http_client:

        def _fetch_and_init(agent_data: dict[str, Any]) -> RemoteA2aAgent | None:
            resource_name = agent_data["agent_resource_name"]
            try:
                card = GDARemoteAgent._fetch_card(
                    resource_name, auth, client=http_client
                )
                return GDARemoteAgent(
                    agent_resource_name=resource_name,
                    description=agent_data.get("description", ""),
                    agent_card=card,
                    google_auth=auth,
                )
            except Exception as e:
                logger.error(
                    "Failed to fetch card or initialize GDARemoteAgent for %s: %s",
                    resource_name,
                    e,
                )
                return None

        with ThreadPoolExecutor(max_workers=max(len(agents), 1)) as executor:
            for res in executor.map(_fetch_and_init, agents):
                if res is not None:
                    subagents.append(res)
    return subagents
