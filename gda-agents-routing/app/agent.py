# ruff: noqa
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os

from dotenv import load_dotenv
from google.adk.agents import Agent
from google.adk.apps import App
from google.adk.models import Gemini
from google.adk.plugins.bigquery_agent_analytics_plugin import (
    BigQueryAgentAnalyticsPlugin,
    BigQueryLoggerConfig,
)
from google.genai import types

from app.config import ROUTER_MODEL
from app.remote_agents import get_remote_agent

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


ROUTER_INSTRUCTION = """You are the enterprise data analytics orchestrator.
Your role is to understand user analytical queries and delegate them to the appropriate specialized data sub-agent.
- Evaluate the question and transfer to the specialized sub-agent whose domain matches the business entities, data tables, and metrics requested.
- For follow-up questions within the same analytical domain, maintain continuity with that sub-agent.
- If no specialized agent covers the requested domain, inform the user about the available data capabilities.
- If it is unclear which data domain the user is asking about, ask the user for clarification.
"""

subagents = get_remote_agent()



root_agent = Agent(
    name="root_agent",
    model=Gemini(
        model=ROUTER_MODEL,
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    instruction=ROUTER_INSTRUCTION,
    sub_agents=subagents,
)


# Initialize BigQuery Analytics
_plugins = []
_project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
_dataset_id = os.environ.get("BQ_ANALYTICS_DATASET_ID", "adk_agent_analytics")
_location = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-east1")

if _project_id:
    try:
        _plugins.append(
            BigQueryAgentAnalyticsPlugin(
                project_id=_project_id,
                dataset_id=_dataset_id,
                location=_location,
                config=BigQueryLoggerConfig(
                    gcs_bucket_name=os.environ.get("BQ_ANALYTICS_GCS_BUCKET"),
                    connection_id=os.environ.get("BQ_ANALYTICS_CONNECTION_ID"),
                    batch_size=10,
                    create_views=False,
                    queue_max_size=10000,
                ),
            )
        )
    except Exception as e:
        logging.warning(f"Failed to initialize BigQuery Analytics: {e}")

app = App(
    root_agent=root_agent,
    name="app",
    plugins=_plugins,
)
