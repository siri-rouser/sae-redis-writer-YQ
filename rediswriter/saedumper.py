import time
import heapq
import pybase64
import logging
from typing import Any, Optional, Tuple

from prometheus_client import Histogram, Summary
from visionapi_yq.messages_pb2 import SaeMessage, VideoFrame
from visionlib.pipeline.publisher import RedisPublisher
from visionlib.saedump import DumpMeta, Event, message_splitter

from .config import RedisWriterConfig

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('redis_writer_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
PROTO_SERIALIZATION_DURATION = Summary('redis_writer_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('redis_writer_proto_deserialization_duration', 'The time it takes to deserialize an input proto')


class StreamEvent:
    def __init__(self, event: Event, stream_start_time: float, file_iter):
        self.event = event
        self.stream_start_time = stream_start_time
        self.file_iter = file_iter

    def __lt__(self, other):
        return (self.event.meta.record_time - self.stream_start_time) < \
               (other.event.meta.record_time - other.stream_start_time)


class Saedumper:
    def __init__(self, config: RedisWriterConfig) -> None:
        self.config = config
        logger.setLevel(self.config.log_level.value)
        self.saedump_files = config.saedump_files
        self.streams_iterators = []
        self.heap = []
        self.setup()
        self.playback_start = time.time()

    def setup(self):
        for file in self.saedump_files:
            logger.info(f'Loading saedump file: {file}')
            file_iter = self._stream_events_generator(file)
            self.streams_iterators.append(file_iter)
            try:
                event, start_time = next(file_iter)
                heapq.heappush(self.heap, StreamEvent(event, start_time, file_iter))
            except StopIteration:
                continue

    def _stream_events_generator(self, file_path: str):
        with open(file_path, 'r') as file:
            messages = message_splitter(file)
            dump_meta = DumpMeta.model_validate_json(next(messages))
            for msg in messages:
                event = Event.model_validate_json(msg)
                yield event, dump_meta.start_time

    def _set_frame_timestamp_to_now(self,proto_bytes: bytes) -> bytes:
        proto = SaeMessage()
        proto.ParseFromString(proto_bytes)
        proto.frame.timestamp_utc_ms = time.time_ns() // 1_000_000
        return proto.SerializeToString()
    
    @GET_DURATION.time()
    def get(self) -> Optional[Tuple[bytes, str]]:
        if not self.heap:
            return None
        
        next_stream_event = heapq.heappop(self.heap)
        event_time_offset = (next_stream_event.event.meta.record_time - next_stream_event.stream_start_time)
        playback_time_offset = time.time() - self.playback_start

        if playback_time_offset < event_time_offset:
            time.sleep(event_time_offset - playback_time_offset)

        proto_bytes = pybase64.standard_b64decode(next_stream_event.event.data_b64)

        if self.config.adjust_timestamp:
            proto_bytes = self._set_frame_timestamp_to_now(proto_bytes)

        try:
            event, _ = next(next_stream_event.file_iter)
            heapq.heappush(self.heap, StreamEvent(event, next_stream_event.stream_start_time, next_stream_event.file_iter))
        except StopIteration:
            pass

        return proto_bytes, next_stream_event.event.meta.source_stream

        
