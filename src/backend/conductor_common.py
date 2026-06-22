"""
Canonical helper source for the Gammavet conductor dedicated-lambda rebuild.

Copy this file into each dedicated lambda repo as `src/backend/conductor_common.py`
and set the lambda-specific ACTOR_LAMBDA value through ConductorRuntime.

This reference preserves the hard-won shared behavior from the current
single-lambda conductor implementation:

- PR #8: fail closed when a start/resume/continue action cannot identify the
  requested route_stop_id.
- PR #9: same-stop no-op.
- PR #10: route_stop_id / pickup_order_id extraction from nota text.
- 04f9a21: route_stop_id self-recovery from session history plus recover-first
  loop-brake ordering.
- Exact-stop completion: pass explicit stop IDs to tenant complete/fail routes
  and block conductor confirmations when the tenant response affects a
  different stop.

The dedicated lambdas must not reintroduce an `accion` enum. The selected node
is the action.
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import requests
from chask_foundation.api.tenant_data_requests import TenantDataClient

try:
    from api.orchestrator_requests import orchestrator_api_manager
except ModuleNotFoundError:  # pragma: no cover - depends on lambda layer layout
    from chask_foundation.api.orchestrator_requests import orchestrator_api_manager

logger = logging.getLogger(__name__)

BOT_PHONE_ID = "1051240901403291"
DEFAULT_TENANT_SLUG = "chask"
DEFAULT_TENANT_BRANCH = "test"

TENANT_START_NEXT_PATH = "gammavet/route-stops/start-next"
TENANT_COMPLETE_CURRENT_PATH = "gammavet/route-stops/complete-current"
TENANT_FAIL_CURRENT_PATH = "gammavet/route-stops/fail-current"
TENANT_PAUSE_DRIVER_PATH = "gammavet/drivers/pause"
TENANT_RESUME_DRIVER_PATH = "gammavet/drivers/resume"

TENANT_START_NEXT_ROUTE = "/api/gammavet/route-stops/start-next"
TENANT_COMPLETE_CURRENT_ROUTE = "/api/gammavet/route-stops/complete-current"
TENANT_FAIL_CURRENT_ROUTE = "/api/gammavet/route-stops/fail-current"

ESTADO_COMPLETADO = "completado"
ESTADO_FALLO = "fallo"

SAFE_ROUTE_CLARIFICATION = (
    "Hola, recibimos tu confirmacion de inicio. Hay una situacion con tu ruta "
    "que necesitamos revisar antes de confirmarte la direccion. "
    "No necesitas responder a este mensaje; el equipo lo revisara y te contactara."
)
COMPLETE_CURRENT_HANDOFF = (
    "No veo una parada activa para cerrar en este momento. "
    "No necesitas responder a este mensaje; el equipo revisara el estado de tu ruta."
)


@dataclass(frozen=True)
class ConductorRuntime:
    """Lambda-specific runtime configuration."""

    actor_lambda: str
    function_uuid_default: str
    tenant_slug_default: str = DEFAULT_TENANT_SLUG
    tenant_branch_default: str = DEFAULT_TENANT_BRANCH
    bot_phone_id: str = BOT_PHONE_ID


@dataclass(frozen=True)
class RouteStopResolution:
    """Resolved stop IDs and their source."""

    route_stop_id: str | None
    pickup_order_id: str | None
    source: str


@dataclass(frozen=True)
class MissingRouteStopDecision:
    """Decision for fail-closed / loop-brake route-stop handling."""

    should_stop: bool
    result_message: str | None = None


def normalizar_telefono(telefono: Any) -> str:
    return "".join(c for c in str(telefono or "") if c.isdigit())


def extract_uuid_field(text: Any, field_name: str) -> str | None:
    if not text:
        return None
    pattern = (
        rf"\b{re.escape(field_name)}\b\s*[:=]\s*"
        r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})"
    )
    match = re.search(pattern, str(text))
    return match.group(1) if match else None


class ConductorContext:
    """Shared adapter around OrchestrationEvent for dedicated conductor lambdas."""

    def __init__(self, evento_orquestacion: Any, runtime: ConductorRuntime):
        self.evento_orquestacion = evento_orquestacion
        self.runtime = runtime

    # ------------------------------------------------------------------
    # Tenant client and request payloads
    # ------------------------------------------------------------------

    def tenant_client(self) -> TenantDataClient:
        explicit_branch = os.environ.get("TENANT_BRANCH") or os.environ.get("CHASK_TENANT_BRANCH")
        base_url_hint = os.environ.get("CHASK_API_BASE_URL") or os.environ.get("BASE_DOMAIN", "")
        if explicit_branch:
            branch = explicit_branch
        elif "app.chask.it" in base_url_hint:
            branch = self.runtime.tenant_branch_default
        else:
            branch = getattr(self.evento_orquestacion, "branch", None) or self.runtime.tenant_branch_default
        slug = os.environ.get("TENANT_SLUG") or self.runtime.tenant_slug_default
        logger.info(
            "TenantDataClient config branch=%s slug=%s lambda_uuid=%s access_token_present=%s",
            branch,
            slug,
            self.function_uuid(),
            bool(getattr(self.evento_orquestacion, "access_token", None)),
        )
        client = TenantDataClient(
            org_uuid=self.evento_orquestacion.organization.organization_id,
            branch=branch,
            lambda_uuid=self.function_uuid(),
            access_token=getattr(self.evento_orquestacion, "access_token", None),
        )
        client._slug = slug
        return client

    def function_uuid(self) -> str:
        return (
            os.getenv("FUNCTION_UUID")
            or os.getenv("CHASK_FUNCTION_UUID")
            or self.runtime.function_uuid_default
        )

    def build_driver_action_payload(self, *, require_ticket: bool = True) -> dict[str, Any]:
        event_uuid = self.event_uuid()
        args = self.tool_args()
        payload: dict[str, Any] = {
            "orchestration_event_uuid": str(event_uuid),
            "source_event_uuid": str(event_uuid),
            "actor_lambda": self.runtime.actor_lambda,
        }

        driver_id = self.first_value(args, "driver_id", "conductor_id")
        driver_phone = (
            self.first_value(args, "driver_phone", "telefono_conductor", "telefono", "phone")
            or self.event_phone()
        )
        if driver_id:
            payload["driver_id"] = str(driver_id).strip()
        if driver_phone:
            payload["driver_phone"] = str(driver_phone).strip()

        ticket_id = self.first_value(args, "ticket_id") or getattr(
            self.evento_orquestacion, "orchestration_session_uuid", None
        )
        if ticket_id:
            payload["ticket_id"] = str(ticket_id)
        elif require_ticket:
            raise ValueError("No se encontro orchestration_session_uuid en el evento")

        if "driver_id" not in payload and "driver_phone" not in payload:
            raise ValueError("No se encontro driver_id ni driver_phone para el conductor")
        return payload

    # ------------------------------------------------------------------
    # Route-stop ID recovery, fail-closed guard, and loop brake
    # ------------------------------------------------------------------

    def resolve_route_stop_ids(self) -> RouteStopResolution:
        """Resolve explicit IDs first, then nota, then same-session history."""

        extra_params = self.evento_orquestacion.extra_params or {}
        sanitized_payload = extra_params.get("sanitized_payload")
        if isinstance(sanitized_payload, dict):
            route_stop_id = sanitized_payload.get("route_stop_id")
            pickup_order_id = sanitized_payload.get("pickup_order_id")
            if route_stop_id or pickup_order_id:
                return RouteStopResolution(
                    str(route_stop_id) if route_stop_id else None,
                    str(pickup_order_id) if pickup_order_id else None,
                    "sanitized_payload",
                )

        args = self.tool_args()
        route_stop_id = args.get("route_stop_id")
        pickup_order_id = args.get("pickup_order_id")
        if route_stop_id or pickup_order_id:
            return RouteStopResolution(
                str(route_stop_id) if route_stop_id else None,
                str(pickup_order_id) if pickup_order_id else None,
                "tool_args",
            )

        nota = str(args.get("nota") or "")
        route_stop_id = extract_uuid_field(nota, "route_stop_id")
        pickup_order_id = extract_uuid_field(nota, "pickup_order_id")
        if route_stop_id or pickup_order_id:
            return RouteStopResolution(route_stop_id, pickup_order_id, "nota")

        history_route_stop_id = self.route_stop_id_from_session_history()
        return RouteStopResolution(history_route_stop_id, pickup_order_id, "session_history")

    def require_route_stop_or_terminal(self, *, action_name: str) -> MissingRouteStopDecision:
        """Recover first; only then fail-closed or apply the loop brake.

        Dedicated start-like lambdas call this before tenant start-next. If the
        returned decision has should_stop=True, the lambda must return the
        result_message and must not call tenant APIs.
        """

        resolved = self.resolve_route_stop_ids()
        if resolved.route_stop_id:
            return MissingRouteStopDecision(should_stop=False)

        if self.prior_route_stop_missing_terminal_seen():
            return MissingRouteStopDecision(
                should_stop=True,
                result_message=self.post_missing_route_stop_terminal_brake(action_name),
            )

        self.fail_closed_route_stop_guard(
            "conductor_route_stop_id_missing_terminal",
            {
                "reason": "missing_requested_route_stop_id",
                "driver_phone": self.driver_phone(),
                "session_uuid": str(self.evento_orquestacion.orchestration_session_uuid),
                "event_id": str(self.evento_orquestacion.event_id),
                "terminal": True,
                "action": action_name,
            },
        )
        return MissingRouteStopDecision(
            should_stop=True,
            result_message=(
                "No se encontro route_stop_id para ejecutar la accion de ruta. "
                "Handoff terminal enviado al conductor."
            ),
        )

    def route_stop_id_from_session_history(self) -> str | None:
        session_uuid = self.evento_orquestacion.orchestration_session_uuid
        if not session_uuid:
            return None
        try:
            response = orchestrator_api_manager.call(
                "get_orchestration_events",
                orchestration_session_id=str(session_uuid),
                access_token=self.evento_orquestacion.access_token,
                organization_id=self.evento_orquestacion.organization.organization_id,
            )
        except Exception as exc:  # pragma: no cover - external failure path
            logger.error("Error obteniendo historial para route_stop_id: %s", exc)
            return None

        events = self.history_events_from_response(response)
        if not events:
            return None

        current_event_id = str(getattr(self.evento_orquestacion, "event_id", "") or "")
        current_pickup_order_id = self.resolve_pickup_order_id_without_history()
        current_driver_phone = normalizar_telefono(self.driver_phone())
        candidates: list[tuple[int, int, str, dict[str, Any]]] = []

        for recency_index, event in enumerate(reversed(events)):
            event_data = self.event_mapping(event)
            if str(event_data.get("event_id") or event_data.get("uuid") or "") == current_event_id:
                continue
            for text in self.history_texts(event_data):
                route_stop_id = extract_uuid_field(text, "route_stop_id")
                if not route_stop_id:
                    continue
                pickup_order_id = extract_uuid_field(text, "pickup_order_id")
                text_phone = normalizar_telefono(text)
                phone_matches = bool(current_driver_phone and current_driver_phone in text_phone)
                pickup_matches = bool(
                    current_pickup_order_id and pickup_order_id == current_pickup_order_id
                )
                score = 0
                if pickup_matches:
                    score += 4
                if phone_matches:
                    score += 2
                if pickup_order_id:
                    score += 1
                candidates.append(
                    (
                        score,
                        -recency_index,
                        route_stop_id,
                        {
                            "pickup_order_id": pickup_order_id,
                            "phone_matches": phone_matches,
                            "source_event_id": event_data.get("event_id") or event_data.get("uuid"),
                        },
                    )
                )

        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        score, _recency, route_stop_id, metadata = candidates[0]
        logger.info(
            "Recovered route_stop_id from session history route_stop_id=%s score=%s metadata=%s",
            route_stop_id,
            score,
            metadata,
        )
        return route_stop_id

    def resolve_pickup_order_id_without_history(self) -> str | None:
        extra_params = self.evento_orquestacion.extra_params or {}
        sanitized_payload = extra_params.get("sanitized_payload")
        if isinstance(sanitized_payload, dict) and sanitized_payload.get("pickup_order_id"):
            return str(sanitized_payload.get("pickup_order_id"))
        args = self.tool_args()
        if args.get("pickup_order_id"):
            return str(args.get("pickup_order_id"))
        return extract_uuid_field(str(args.get("nota") or ""), "pickup_order_id")

    def prior_route_stop_missing_terminal_seen(self) -> bool:
        session_uuid = self.evento_orquestacion.orchestration_session_uuid
        if not session_uuid:
            return False
        try:
            response = orchestrator_api_manager.call(
                "get_orchestration_events",
                orchestration_session_id=str(session_uuid),
                access_token=self.evento_orquestacion.access_token,
                organization_id=self.evento_orquestacion.organization.organization_id,
            )
        except Exception as exc:  # pragma: no cover - external failure path
            logger.error("Error obteniendo historial para terminal brake: %s", exc)
            return False

        current_event_id = str(getattr(self.evento_orquestacion, "event_id", "") or "")
        for event in self.history_events_from_response(response):
            event_data = self.event_mapping(event)
            if str(event_data.get("event_id") or event_data.get("uuid") or "") == current_event_id:
                continue
            extra_params = event_data.get("extra_params") or {}
            if isinstance(extra_params, dict):
                metadata = extra_params.get("metadata") or {}
                if not isinstance(metadata, dict):
                    metadata = {}
                marker = extra_params.get("event_type") or metadata.get("event_type")
                if marker == "conductor_route_stop_id_missing_terminal":
                    return True
                if (
                    metadata.get("terminal") is True
                    and "route_stop_id_missing" in str(event_data.get("prompt") or marker)
                ):
                    return True
            for text in self.history_texts(event_data):
                if "conductor_route_stop_id_missing_terminal" in text:
                    return True
        return False

    def post_missing_route_stop_terminal_brake(self, action_name: str) -> str:
        self.emit_dispatch_event(
            event_type="conductor_post_missing_route_stop_terminal_brake",
            metadata={
                "reason": "prior_conductor_route_stop_id_missing_terminal",
                "attempted_action": action_name,
                "driver_phone": self.driver_phone(),
                "session_uuid": str(self.evento_orquestacion.orchestration_session_uuid),
                "event_id": str(self.evento_orquestacion.event_id),
                "terminal": True,
            },
        )
        return (
            "Handoff terminal previo por falta de route_stop_id. "
            "No se ejecuto accion de conductor."
        )

    def fail_closed_route_stop_guard(self, event_type: str, metadata: dict[str, Any]) -> None:
        self.emit_dispatch_event(event_type=event_type, metadata=metadata)
        self.enviar_mensaje_texto(SAFE_ROUTE_CLARIFICATION)

    # ------------------------------------------------------------------
    # Start-next and completion verification
    # ------------------------------------------------------------------

    def parse_start_next_result(self, response: Any) -> tuple[bool, Any]:
        if isinstance(response, dict) and "route_stop" in response:
            return bool(response.get("claimed")), response.get("route_stop")
        if isinstance(response, dict) and "next_route_stop" in response:
            return bool(response.get("claimed")), response.get("next_route_stop")
        return True, response

    def stop_id_from_start_next_result(self, response: Any, parsed_stop: dict[str, Any]) -> Any:
        if isinstance(response, dict):
            if response.get("current_route_stop_id"):
                return response.get("current_route_stop_id")
            route_stop = response.get("route_stop")
            if isinstance(route_stop, dict) and route_stop.get("id"):
                return route_stop.get("id")
            next_route_stop = response.get("next_route_stop")
            if isinstance(next_route_stop, dict) and next_route_stop.get("id"):
                return next_route_stop.get("id")
        return parsed_stop.get("id")

    def emit_same_stop_noop(
        self,
        *,
        route_stop_id: Any,
        action_name: str,
        claimed: bool,
    ) -> None:
        self.emit_dispatch_event(
            event_type="conductor_same_stop_noop",
            metadata={
                "route_stop_id": str(route_stop_id),
                "driver_phone": self.driver_phone(),
                "session_uuid": str(self.evento_orquestacion.orchestration_session_uuid),
                "event_id": str(self.evento_orquestacion.event_id),
                "claimed": bool(claimed),
                "action": action_name,
            },
        )

    def emit_stale_start_block(
        self,
        *,
        requested_route_stop_id: Any,
        actual_route_stop_id: Any,
        stop: dict[str, Any],
        claimed: bool,
    ) -> None:
        self.fail_closed_route_stop_guard(
            "conductor_stale_stop_blocked",
            {
                "requested_route_stop_id": str(requested_route_stop_id),
                "actual_en_ruta_route_stop_id": str(actual_route_stop_id),
                "actual_en_ruta_clinic": stop.get("clinic_name_snapshot"),
                "driver_phone": self.driver_phone(),
                "session_uuid": str(self.evento_orquestacion.orchestration_session_uuid),
                "event_id": str(self.evento_orquestacion.event_id),
                "claimed": bool(claimed),
            },
        )

    def completion_response_mismatched(
        self,
        completed_stop: dict[str, Any],
        *,
        requested_route_stop_id: Any,
        requested_pickup_order_id: Any,
    ) -> bool:
        if not requested_route_stop_id and not requested_pickup_order_id:
            return False
        if not isinstance(completed_stop, dict) or not completed_stop:
            return True
        actual_route_stop_id = completed_stop.get("id")
        actual_pickup_order_id = completed_stop.get("pickup_order_id")
        if requested_route_stop_id and str(actual_route_stop_id) != str(requested_route_stop_id):
            return True
        if (
            requested_pickup_order_id
            and str(actual_pickup_order_id) != str(requested_pickup_order_id)
        ):
            return True
        return False

    def emit_completion_mismatch(
        self,
        completed_stop: dict[str, Any],
        *,
        requested_route_stop_id: Any,
        requested_pickup_order_id: Any,
        outcome: str,
        endpoint_route: str,
    ) -> None:
        self.emit_dispatch_event(
            event_type="conductor_completion_route_stop_mismatch",
            metadata={
                "reason": "tenant_completed_different_stop",
                "requested_route_stop_id": (
                    str(requested_route_stop_id) if requested_route_stop_id else None
                ),
                "requested_pickup_order_id": (
                    str(requested_pickup_order_id) if requested_pickup_order_id else None
                ),
                "actual_route_stop_id": (
                    str(completed_stop.get("id")) if isinstance(completed_stop, dict) else None
                ),
                "actual_pickup_order_id": (
                    str(completed_stop.get("pickup_order_id"))
                    if isinstance(completed_stop, dict) and completed_stop.get("pickup_order_id")
                    else None
                ),
                "outcome": outcome,
                "endpoint": endpoint_route,
                "terminal": True,
            },
        )

    def complete_current_missing_terminal(self, exc: requests.HTTPError) -> str:
        self.emit_dispatch_event(
            event_type="conductor_complete_current_missing_terminal",
            metadata={
                "reason": "complete_current_no_active_stop",
                "driver_phone": self.driver_phone(),
                "session_uuid": str(self.evento_orquestacion.orchestration_session_uuid),
                "event_id": str(self.evento_orquestacion.event_id),
                "terminal": True,
                "tenant_error": str(exc),
            },
        )
        self.enviar_mensaje_texto(COMPLETE_CURRENT_HANDOFF)
        return (
            "No hay parada en ruta o pausada para completar. "
            "Handoff terminal enviado al conductor."
        )

    @staticmethod
    def is_no_active_stop_http_404(exc: requests.HTTPError) -> bool:
        response = getattr(exc, "response", None)
        if getattr(response, "status_code", None) != 404:
            return False
        message = str(exc).lower()
        return (
            "no in-route or paused stop" in message
            or "no hay parada" in message
            or "sin parada" in message
        )

    # ------------------------------------------------------------------
    # WhatsApp / dispatch events
    # ------------------------------------------------------------------

    def enviar_mensaje_texto(self, texto: str) -> None:
        user_phone, agent_phone = self.phones_para_respuesta()
        if not user_phone or not agent_phone:
            logger.warning(
                "No se puede enviar mensaje de texto: user_phone=%s agent_phone=%s",
                user_phone,
                agent_phone,
            )
            return
        self.dispatch_response_to_whatsapp(
            texto,
            {"user_phone_number": user_phone, "agent_phone_number": agent_phone},
        )

    def dispatch_response_to_whatsapp(self, texto: str, extra_params: dict[str, Any]) -> None:
        try:
            evolve_response = orchestrator_api_manager.call(
                "evolve_event",
                parent_event_uuid=str(self.evento_orquestacion.event_id),
                event_type="response_to_whatsapp_message",
                source="agent",
                target="orchestrator",
                prompt=texto,
                extra_params=extra_params,
                access_token=self.evento_orquestacion.access_token,
                organization_id=self.evento_orquestacion.organization.organization_id,
            )
            if evolve_response.get("status_code") not in (200, 201):
                raise RuntimeError(f"Failed to evolve event: {evolve_response.get('error')}")
            evolved_uuid = evolve_response.get("uuid")
            if not evolved_uuid:
                raise RuntimeError("API response missing uuid")

            wa_event = self.evento_orquestacion.model_copy(deep=True)
            wa_event.event_id = evolved_uuid
            wa_event.event_type = "response_to_whatsapp_message"
            wa_event.source = "agent"
            wa_event.target = "orchestrator"
            wa_event.prompt = texto
            wa_event.extra_params = evolve_response.get("extra_params", extra_params)

            orchestrator_api_manager.call(
                "forward_oe_to_kafka",
                orchestration_event=wa_event.model_dump(),
                topic="orchestrator",
                access_token=wa_event.access_token,
                organization_id=wa_event.organization.organization_id,
            )
        except Exception as exc:  # pragma: no cover - external failure path
            logger.error("Error enviando mensaje WhatsApp: %s", exc)

    def emit_dispatch_event(self, *, event_type: str, metadata: dict[str, Any]) -> None:
        try:
            orchestrator_api_manager.call(
                "evolve_event",
                parent_event_uuid=str(self.evento_orquestacion.event_id),
                event_type="dispatch_event",
                source="agent",
                target="orchestrator",
                prompt=event_type,
                extra_params={
                    "event_type": event_type,
                    "actor_lambda": self.runtime.actor_lambda,
                    "metadata": metadata,
                },
                access_token=self.evento_orquestacion.access_token,
                organization_id=self.evento_orquestacion.organization.organization_id,
            )
        except Exception as exc:  # pragma: no cover - external failure path
            logger.error("Error emitiendo dispatch_event %s: %s", event_type, exc)

    # ------------------------------------------------------------------
    # Event parsing helpers
    # ------------------------------------------------------------------

    def event_uuid(self) -> UUID:
        raw_event_id = getattr(self.evento_orquestacion, "event_id", None)
        try:
            return UUID(str(raw_event_id))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Invalid orchestration_event.event_id: expected UUID before sending "
                f"source_event_uuid, got {raw_event_id!r}"
            ) from exc

    def event_phone(self) -> str:
        customer = getattr(self.evento_orquestacion, "customer", None)
        if customer and getattr(customer, "phone", None):
            return str(customer.phone).strip()

        extra_params = self.evento_orquestacion.extra_params or {}
        value = str(
            self.first_value(extra_params, "driver_phone", "user_phone_number", "phone", "from")
            or ""
        ).strip()
        if value:
            return value

        prompt = str(getattr(self.evento_orquestacion, "prompt", "") or "")
        digits = "".join(re.findall(r"\d+", prompt))
        return digits if len(digits) >= 8 else ""

    def driver_phone(self) -> str:
        user_phone, _agent_phone = self.phones_para_respuesta()
        return user_phone or self.event_phone()

    def phones_para_respuesta(self) -> tuple[str | None, str | None]:
        extra_params = self.evento_orquestacion.extra_params or {}
        user_phone = extra_params.get("user_phone_number") or self.event_phone()
        agent_phone = extra_params.get("agent_phone_number") or self.runtime.bot_phone_id
        if not user_phone or not agent_phone:
            phones = self.obtener_phones_de_sesion()
            user_phone = user_phone or phones.get("user_phone_number")
            agent_phone = agent_phone or phones.get("agent_phone_number")
        return (normalizar_telefono(user_phone) if user_phone else None, agent_phone)

    def obtener_phones_de_sesion(self) -> dict[str, str]:
        session_uuid = self.evento_orquestacion.orchestration_session_uuid
        if not session_uuid:
            return {}
        try:
            response = orchestrator_api_manager.call(
                "get_orchestration_events",
                orchestration_session_id=str(session_uuid),
                access_token=self.evento_orquestacion.access_token,
                organization_id=self.evento_orquestacion.organization.organization_id,
            )
        except Exception as exc:  # pragma: no cover - external failure path
            logger.error("Error obteniendo phones de sesion: %s", exc)
            return {}

        for ev in self.history_events_from_response(response):
            event_data = self.event_mapping(ev)
            if event_data.get("event_type") != "new_ticket":
                continue
            extra_params = event_data.get("extra_params") or {}
            phones: dict[str, str] = {}
            if extra_params.get("user_phone_number"):
                phones["user_phone_number"] = extra_params["user_phone_number"]
            if extra_params.get("agent_phone_number"):
                phones["agent_phone_number"] = extra_params["agent_phone_number"]
            if phones:
                return phones
        return {}

    def tool_args(self) -> dict[str, Any]:
        params_extra = self.evento_orquestacion.extra_params or {}
        llamadas = params_extra.get("tool_calls", [])
        if not llamadas:
            return {}
        args = llamadas[0].get("args", {}) or {}
        return args if isinstance(args, dict) else {}

    @staticmethod
    def first_value(data: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            value = data.get(key)
            if value:
                return value
        return None

    @staticmethod
    def history_events_from_response(response: Any) -> list[Any]:
        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            events = response.get("orchestration_events", [])
            return events if isinstance(events, list) else []
        return []

    @staticmethod
    def event_mapping(event: Any) -> dict[str, Any]:
        if isinstance(event, dict):
            return event
        if hasattr(event, "model_dump"):
            return event.model_dump()
        if hasattr(event, "dict"):
            return event.dict()
        return {}

    def history_texts(self, event: dict[str, Any]) -> list[str]:
        texts: list[str] = []
        for key in ("prompt", "plan", "response", "content", "message"):
            value = event.get(key)
            if isinstance(value, str) and value.strip():
                texts.append(value)
        extra_params = event.get("extra_params") or {}
        if isinstance(extra_params, dict):
            texts.extend(self.strings_from_tool_calls(extra_params.get("tool_calls", [])))
            for key in ("execute_plan", "function_call_response", "mensaje", "message", "text"):
                value = extra_params.get(key)
                if isinstance(value, str) and value.strip():
                    texts.append(value)
        return texts

    @staticmethod
    def strings_from_tool_calls(tool_calls: Any) -> list[str]:
        texts: list[str] = []
        if not isinstance(tool_calls, list):
            return texts
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            args = call.get("args") or {}
            if not isinstance(args, dict):
                continue
            for key in ("mensaje", "message", "nota", "text", "prompt"):
                value = args.get(key)
                if isinstance(value, str) and value.strip():
                    texts.append(value)
        return texts


@contextmanager
def tenant_data_public_test_mode() -> Iterator[None]:
    """Compatibility shim for the currently published foundation helper."""

    previous_mode = os.environ.get("MODE")
    os.environ["MODE"] = "PRODUCTION"
    try:
        yield
    finally:
        if previous_mode is None:
            os.environ.pop("MODE", None)
        else:
            os.environ["MODE"] = previous_mode
