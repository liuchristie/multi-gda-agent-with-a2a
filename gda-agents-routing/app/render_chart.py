import base64
import json
import logging
from typing import Any
import vl_convert as vlc

logger = logging.getLogger(__name__)

def apply_google_theme(vega_config: dict[str, Any] | None) -> dict[str, Any] | None:
    # Inject Google palette into standard spec encoding without in-place mutation
    if not isinstance(vega_config, dict):
        return vega_config
    encoding = vega_config.get("encoding")
    if isinstance(encoding, dict) and "color" not in encoding:
        vega_config = dict(vega_config)
        vega_config["encoding"] = dict(encoding, color={"value": "#1a73e8"})  # Standard Google Blue
    return vega_config


def extract_vega_config(chart_spec: dict[str, Any] | str) -> dict[str, Any] | None:
    """Safely extracts standard Vega-Lite configuration dictionary."""
    if isinstance(chart_spec, str):
        try:
            chart_spec = json.loads(chart_spec)
        except Exception:
            return None

    if "chart" in chart_spec and isinstance(chart_spec["chart"], dict):
        return chart_spec["chart"].get("result", {}).get("vega_config")
    if "result" in chart_spec and isinstance(chart_spec["result"], dict):
        return chart_spec["result"].get("vega_config")

    if any(k in chart_spec for k in ("mark", "encoding", "$schema")):
        return chart_spec

    return chart_spec.get("vega_config") or chart_spec.get("vegaConfig")


def render_chart_to_png(vega_config: dict[str, Any]) -> bytes | None:
    """Renders standard Vega-Lite spec directly to PNG using vl-convert."""
    try:
        return vlc.vegalite_to_png(vl_spec=vega_config)
    except Exception as e:
        logger.error("Failed to render chart to PNG: %s", e)
        return None


def render_chart_markdown(chart_spec: dict[str, Any]) -> str | None:
    """Renders chart to a Markdown image string."""
    # Fast path if already an extracted Vega-Lite spec
    if isinstance(chart_spec, dict) and any(k in chart_spec for k in ("mark", "encoding", "$schema")):
        vega_config = chart_spec
    else:
        vega_config = extract_vega_config(chart_spec)

    if not vega_config:
        return None

    # Extract Title (handling dictionaries)
    title = vega_config.get("title", "Data Visualization")
    if isinstance(title, dict):
        title = title.get("text", "Data Visualization")

    # Draw using vl-convert 
    png_bytes = render_chart_to_png(vega_config)
    if png_bytes:
        encoded_image = base64.b64encode(png_bytes).decode("utf-8")
        return f"\n\n![{title}](data:image/png;base64,{encoded_image})\n\n"
        
    return None
