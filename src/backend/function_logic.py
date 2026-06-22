"""Business logic for the dedicated Gammavet route-resume lambda."""

from __future__ import annotations

import logging
from typing import Any

from chask_foundation.backend.models import OrchestrationEvent

from .conductor_common import (
    TENANT_RESUME_DRIVER_PATH,
    TENANT_START_NEXT_PATH,
    TENANT_START_NEXT_ROUTE,
    ConductorContext,
    ConductorRuntime,
    tenant_data_public_test_mode,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ACTION_NAME = "reanudar"
ACTOR_LAMBDA = "gammavet_reanudar_ruta"
DEFAULT_FUNCTION_UUID = "00000000-0000-4000-8000-000000000003"


class FunctionBackend:
    """Clear pause state and claim the exact intended route stop."""

    def __init__(self, orchestration_event: OrchestrationEvent):
        self.orchestration_event = orchestration_event
        self.context = ConductorContext(
            orchestration_event,
            ConductorRuntime(
                actor_lambda=ACTOR_LAMBDA,
                function_uuid_default=DEFAULT_FUNCTION_UUID,
            ),
        )
        logger.info(
            "Initialized ReanudarRutaFn for org=%s",
            orchestration_event.organization.organization_id,
        )

    def process_request(self) -> str:
        _resume_driver(self.context)
        return _start_requested_stop(
            self.context,
            action_name=ACTION_NAME,
            message_prefix="Ruta reanudada. Siguiente parada",
            started_label="reanudada",
        )


def _resume_driver(context: ConductorContext) -> None:
    payload = context.build_driver_action_payload(require_ticket=False)
    nota = str(context.tool_args().get("nota") or "").strip()
    if nota:
        payload["note"] = nota
    with tenant_data_public_test_mode():
        result = context.tenant_client().post(TENANT_RESUME_DRIVER_PATH, json=payload)
    if not isinstance(result, dict):
        raise RuntimeError("Tenant API /api/gammavet/drivers/resume devolvio una respuesta inesperada")


def _start_requested_stop(
    context: ConductorContext,
    *,
    action_name: str,
    message_prefix: str,
    started_label: str,
) -> str:
    decision = context.require_route_stop_or_terminal(action_name=action_name)
    if decision.should_stop:
        return decision.result_message or "Accion de ruta detenida por guardia terminal."

    resolved = context.resolve_route_stop_ids()
    requested_route_stop_id = resolved.route_stop_id
    requested_pickup_order_id = resolved.pickup_order_id

    payload = context.build_driver_action_payload()
    payload["action"] = action_name
    payload["route_stop_id"] = requested_route_stop_id
    if requested_pickup_order_id:
        payload["pickup_order_id"] = requested_pickup_order_id

    logger.info(
        "Calling start-next action=%s route_stop_id=%s pickup_order_id=%s source=%s",
        action_name,
        requested_route_stop_id,
        requested_pickup_order_id,
        resolved.source,
    )
    with tenant_data_public_test_mode():
        result = context.tenant_client().post(TENANT_START_NEXT_PATH, json=payload)

    claimed, stop = context.parse_start_next_result(result)
    if not claimed and stop is None:
        context.enviar_mensaje_texto(
            "Por el momento no tienes rutas pendientes.\n"
            "Te notificaremos cuando entre una nueva orden en tu zona."
        )
        return "Conductor reanudado. No hay paradas asignadas. Mensaje enviado."
    if not isinstance(stop, dict):
        raise RuntimeError(f"Tenant API {TENANT_START_NEXT_ROUTE} devolvio una respuesta inesperada")

    returned_stop_id = context.stop_id_from_start_next_result(result, stop)
    if requested_route_stop_id and returned_stop_id and str(returned_stop_id) == str(
        requested_route_stop_id
    ) and not claimed:
        context.emit_same_stop_noop(
            route_stop_id=requested_route_stop_id,
            action_name=action_name,
            claimed=claimed,
        )
        return (
            "Conductor reanudado. No-op: la ruta ya estaba en curso en la misma "
            "parada solicitada. No se envio un nuevo WhatsApp al conductor."
        )

    if requested_route_stop_id and str(returned_stop_id) != str(requested_route_stop_id):
        context.emit_stale_start_block(
            requested_route_stop_id=requested_route_stop_id,
            actual_route_stop_id=returned_stop_id,
            stop=stop,
            claimed=claimed,
        )
        return (
            "Conductor reanudado, pero se bloqueo la respuesta de ruta por mismatch "
            "de route_stop_id. Mensaje seguro de aclaracion enviado al conductor."
        )

    context.enviar_mensaje_texto(_stop_text(stop, message_prefix))
    total = stop.get("total_stops") or stop.get("route_total_stops") or "?"
    route_id = stop.get("route_code") or stop.get("route_id") or "?"
    stop_number = stop.get("stop_number") or stop.get("queue_position") or "?"
    return (
        f"Ruta {route_id} {started_label}. "
        f"Mensaje con parada {stop_number}/{total} enviado al conductor."
    )


def _stop_text(stop: dict[str, Any], message_prefix: str) -> str:
    clinic = stop.get("clinic_name_snapshot") or "Clinica"
    address = stop.get("address_snapshot") or "direccion no informada"
    comuna = stop.get("comuna_snapshot") or "comuna no informada"
    maps = stop.get("maps_url_snapshot") or "GPS no informado"
    return f"{message_prefix}:\n\n{clinic}\nDireccion: {address}, {comuna}\nGPS: {maps}"
