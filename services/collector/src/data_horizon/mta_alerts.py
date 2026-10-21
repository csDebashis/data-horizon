"""Read MTA alert presentation metadata without broadening its matching scope."""

from __future__ import annotations

import re
from urllib.parse import unquote, urlsplit

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.protobuf.unknown_fields import UnknownFieldSet


def _mercury_class():
    # A private descriptor avoids changing the shared GTFS-Realtime schema.
    file = descriptor_pb2.FileDescriptorProto(name="mta_mercury_metadata.proto", syntax="proto2")
    message = file.message_type.add(name="MercuryEntitySelector")
    message.field.add(name="sort_order", number=1, label=2, type=9)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(file)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("MercuryEntitySelector"))


_MercuryMetadata = _mercury_class()


def official_subway_alerts_source(source_url, configured_url):
    """Apply the agency alias only to the configured official subway alert feed."""
    if not source_url or source_url != configured_url:
        return False
    parsed = urlsplit(source_url)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "api-endpoint.mta.info"
        and unquote(parsed.path) == "/Dataservice/mtagtfsfeeds/camsys/subway-alerts"
        and not parsed.query
        and not parsed.fragment
    )


def _sort_order(payload):
    if not isinstance(payload, bytes) or len(payload) > 512:
        raise ValueError("Invalid Mercury metadata size")
    message = _MercuryMetadata()
    try:
        message.ParseFromString(payload)
    except DecodeError as exc:
        raise ValueError("Invalid Mercury metadata") from exc
    if (
        not message.IsInitialized()
        or list(UnknownFieldSet(message))
        or message.SerializeToString(deterministic=True) != payload
        or not isinstance(message.sort_order, str)
        or not re.fullmatch(r"[A-Za-z0-9_.-]+(?::[A-Za-z0-9_.-]+)*:(?:[1-9]|[12][0-9]|3[0-5])", message.sort_order)
        or len(message.sort_order.encode("utf-8")) > 200
    ):
        raise ValueError("Unsupported Mercury metadata")
    return message.sort_order


def alert_selector(selector, *, official_source=False):
    """Return matching clauses and separate optional source/display metadata.

    Only the documented extension 1001, containing its required string field 1,
    is presentation-only. Other unknown fields, including nested trip fields,
    retain a restrictive marker recognised by the shared alert matcher.
    """
    item = MessageToDict(selector, preserving_proto_field_name=True, use_integers_for_enums=True)
    metadata = {}
    unknown = list(UnknownFieldSet(selector))
    try:
        if unknown:
            if (
                not official_source or len(unknown) != 1
                or unknown[0].field_number != 1001 or unknown[0].wire_type != 2
            ):
                raise ValueError("Unknown selector restriction")
            metadata["sort_order"] = _sort_order(unknown[0].data)
        # DiscardUnknownFields is recursive. Compare every known nested message
        # separately so the permitted top-level extension cannot hide new scope.
        for field, value in selector.ListFields():
            if field.message_type is not None:
                messages = value if field.is_repeated else [value]
                for message in messages:
                    known = type(message)()
                    known.CopyFrom(message)
                    known.DiscardUnknownFields()
                    if known.SerializeToString(deterministic=True) != message.SerializeToString(deterministic=True):
                        raise ValueError("Unknown nested selector restriction")
    except ValueError:
        item["unsupported_wire_restriction"] = True
        metadata = {}
    if official_source and item.get("agency_id") == "MTASBWY":
        item["agency_id"] = "MTA NYCT"
        metadata.update(source_agency_id="MTASBWY", canonical_agency_id="MTA NYCT")
    return item, metadata
