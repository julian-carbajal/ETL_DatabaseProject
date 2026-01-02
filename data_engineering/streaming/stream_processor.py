"""
Real-time Stream Processing Framework
======================================

Simulates Kafka/Redis streaming patterns for real-time data processing.
Can be extended to use actual Kafka/Redis in production.
"""

import json
import time
import threading
import queue
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class StreamStatus(Enum):
    """Stream processing status."""
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    ERROR = "ERROR"


@dataclass
class StreamConfig:
    """Configuration for stream processing."""
    name: str
    batch_size: int = 100
    batch_timeout_seconds: float = 5.0
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    enable_checkpointing: bool = True
    checkpoint_interval: int = 1000
    dead_letter_queue: bool = True


@dataclass
class StreamMessage:
    """A message in the stream."""
    id: str
    topic: str
    key: Optional[str]
    value: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    headers: Dict[str, str] = field(default_factory=dict)
    partition: int = 0
    offset: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic,
            "key": self.key,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "headers": self.headers,
            "partition": self.partition,
            "offset": self.offset,
        }


@dataclass
class StreamMetrics:
    """Metrics for stream processing."""
    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    batches_processed: int = 0
    processing_time_ms: float = 0.0
    last_offset: int = 0
    lag: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages_received": self.messages_received,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "batches_processed": self.batches_processed,
            "avg_processing_time_ms": (
                self.processing_time_ms / self.batches_processed 
                if self.batches_processed > 0 else 0
            ),
            "last_offset": self.last_offset,
            "lag": self.lag,
        }


class InMemoryMessageBroker:
    """
    In-memory message broker simulating Kafka/Redis.
    Replace with actual Kafka/Redis client in production.
    """
    
    def __init__(self):
        self.topics: Dict[str, List[StreamMessage]] = {}
        self.consumer_offsets: Dict[str, Dict[str, int]] = {}
        self.lock = threading.Lock()
        self._offset_counter = 0
    
    def create_topic(self, topic: str, partitions: int = 1):
        """Create a new topic."""
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = []
                logger.info(f"Created topic: {topic}")
    
    def produce(self, topic: str, message: StreamMessage) -> int:
        """Produce a message to a topic."""
        with self.lock:
            if topic not in self.topics:
                self.topics[topic] = []
            
            self._offset_counter += 1
            message.offset = self._offset_counter
            self.topics[topic].append(message)
            
            return message.offset
    
    def consume(
        self,
        topic: str,
        consumer_group: str,
        max_messages: int = 100,
    ) -> List[StreamMessage]:
        """Consume messages from a topic."""
        with self.lock:
            if topic not in self.topics:
                return []
            
            if consumer_group not in self.consumer_offsets:
                self.consumer_offsets[consumer_group] = {}
            
            current_offset = self.consumer_offsets[consumer_group].get(topic, 0)
            
            messages = [
                msg for msg in self.topics[topic]
                if msg.offset > current_offset
            ][:max_messages]
            
            if messages:
                self.consumer_offsets[consumer_group][topic] = messages[-1].offset
            
            return messages
    
    def commit_offset(self, topic: str, consumer_group: str, offset: int):
        """Commit consumer offset."""
        with self.lock:
            if consumer_group not in self.consumer_offsets:
                self.consumer_offsets[consumer_group] = {}
            self.consumer_offsets[consumer_group][topic] = offset
    
    def get_lag(self, topic: str, consumer_group: str) -> int:
        """Get consumer lag."""
        with self.lock:
            if topic not in self.topics:
                return 0
            
            latest_offset = max(
                (msg.offset for msg in self.topics[topic]),
                default=0
            )
            current_offset = self.consumer_offsets.get(consumer_group, {}).get(topic, 0)
            
            return latest_offset - current_offset


# Global broker instance
_broker = InMemoryMessageBroker()


def get_broker() -> InMemoryMessageBroker:
    """Get the global message broker."""
    return _broker


class StreamProcessor:
    """
    Stream processor for real-time data processing.
    
    Supports:
    - Batch processing with configurable size and timeout
    - Exactly-once semantics (simulated)
    - Dead letter queue for failed messages
    - Checkpointing for recovery
    - Metrics collection
    """
    
    def __init__(
        self,
        config: StreamConfig,
        broker: InMemoryMessageBroker = None,
    ):
        self.config = config
        self.broker = broker or get_broker()
        self.status = StreamStatus.IDLE
        self.metrics = StreamMetrics()
        
        self.handlers: Dict[str, Callable] = {}
        self.dead_letter_queue: List[StreamMessage] = []
        self.checkpoints: Dict[str, int] = {}
        
        self._stop_event = threading.Event()
        self._processing_thread: Optional[threading.Thread] = None
        
        self.logger = logging.getLogger(f"{__name__}.{config.name}")
    
    def register_handler(
        self,
        topic: str,
        handler: Callable[[List[StreamMessage]], List[Dict[str, Any]]],
    ):
        """Register a handler for a topic."""
        self.handlers[topic] = handler
        self.broker.create_topic(topic)
        self.logger.info(f"Registered handler for topic: {topic}")
    
    def start(self, consumer_group: str = "default"):
        """Start the stream processor."""
        if self.status == StreamStatus.RUNNING:
            self.logger.warning("Processor already running")
            return
        
        self._stop_event.clear()
        self.status = StreamStatus.RUNNING
        
        self._processing_thread = threading.Thread(
            target=self._process_loop,
            args=(consumer_group,),
            daemon=True,
        )
        self._processing_thread.start()
        
        self.logger.info(f"Stream processor started: {self.config.name}")
    
    def stop(self, timeout: float = 10.0):
        """Stop the stream processor."""
        self._stop_event.set()
        self.status = StreamStatus.STOPPED
        
        if self._processing_thread:
            self._processing_thread.join(timeout=timeout)
        
        self.logger.info(f"Stream processor stopped: {self.config.name}")
    
    def pause(self):
        """Pause processing."""
        self.status = StreamStatus.PAUSED
        self.logger.info("Processor paused")
    
    def resume(self):
        """Resume processing."""
        if self.status == StreamStatus.PAUSED:
            self.status = StreamStatus.RUNNING
            self.logger.info("Processor resumed")
    
    def _process_loop(self, consumer_group: str):
        """Main processing loop."""
        while not self._stop_event.is_set():
            if self.status == StreamStatus.PAUSED:
                time.sleep(0.1)
                continue
            
            for topic, handler in self.handlers.items():
                try:
                    self._process_topic(topic, consumer_group, handler)
                except Exception as e:
                    self.logger.error(f"Error processing topic {topic}: {e}")
                    self.status = StreamStatus.ERROR
            
            time.sleep(0.01)  # Small delay to prevent busy waiting
    
    def _process_topic(
        self,
        topic: str,
        consumer_group: str,
        handler: Callable,
    ):
        """Process messages from a topic."""
        messages = self.broker.consume(
            topic,
            consumer_group,
            max_messages=self.config.batch_size,
        )
        
        if not messages:
            return
        
        self.metrics.messages_received += len(messages)
        
        start_time = time.time()
        
        try:
            # Process batch
            results = handler(messages)
            
            # Update metrics
            self.metrics.messages_processed += len(messages)
            self.metrics.batches_processed += 1
            self.metrics.processing_time_ms += (time.time() - start_time) * 1000
            self.metrics.last_offset = messages[-1].offset
            
            # Commit offset
            self.broker.commit_offset(topic, consumer_group, messages[-1].offset)
            
            # Checkpoint
            if (self.config.enable_checkpointing and 
                self.metrics.messages_processed % self.config.checkpoint_interval == 0):
                self._save_checkpoint(topic, messages[-1].offset)
            
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            self.metrics.messages_failed += len(messages)
            
            # Send to dead letter queue
            if self.config.dead_letter_queue:
                for msg in messages:
                    msg.headers["error"] = str(e)
                    msg.headers["failed_at"] = datetime.utcnow().isoformat()
                    self.dead_letter_queue.append(msg)
        
        # Update lag
        self.metrics.lag = self.broker.get_lag(topic, consumer_group)
    
    def _save_checkpoint(self, topic: str, offset: int):
        """Save processing checkpoint."""
        self.checkpoints[topic] = offset
        self.logger.debug(f"Checkpoint saved: {topic}={offset}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return {
            "config": self.config.name,
            "status": self.status.value,
            **self.metrics.to_dict(),
            "dead_letter_queue_size": len(self.dead_letter_queue),
        }
    
    def process_batch_sync(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        handler: Callable = None,
    ) -> List[Dict[str, Any]]:
        """
        Synchronously process a batch of messages.
        Useful for testing or one-off processing.
        """
        handler = handler or self.handlers.get(topic)
        if not handler:
            raise ValueError(f"No handler registered for topic: {topic}")
        
        # Convert to StreamMessage
        stream_messages = []
        for i, msg in enumerate(messages):
            stream_messages.append(StreamMessage(
                id=f"msg-{i}",
                topic=topic,
                key=msg.get("key"),
                value=msg,
                offset=i,
            ))
        
        return handler(stream_messages)


class WindowedAggregator:
    """
    Windowed aggregation for streaming data.
    Supports tumbling and sliding windows.
    """
    
    def __init__(
        self,
        window_size_seconds: int = 60,
        window_type: str = "tumbling",  # tumbling or sliding
        slide_seconds: int = None,
    ):
        self.window_size = window_size_seconds
        self.window_type = window_type
        self.slide_seconds = slide_seconds or window_size_seconds
        
        self.windows: Dict[str, Dict[str, Any]] = {}
        self.lock = threading.Lock()
    
    def add(self, key: str, value: Any, timestamp: datetime = None):
        """Add a value to the window."""
        timestamp = timestamp or datetime.utcnow()
        window_start = self._get_window_start(timestamp)
        window_key = f"{key}:{window_start}"
        
        with self.lock:
            if window_key not in self.windows:
                self.windows[window_key] = {
                    "key": key,
                    "window_start": window_start,
                    "values": [],
                    "count": 0,
                    "sum": 0,
                }
            
            window = self.windows[window_key]
            window["values"].append(value)
            window["count"] += 1
            
            if isinstance(value, (int, float)):
                window["sum"] += value
    
    def _get_window_start(self, timestamp: datetime) -> int:
        """Get the window start timestamp."""
        epoch = timestamp.timestamp()
        return int(epoch // self.window_size) * self.window_size
    
    def get_window(self, key: str, timestamp: datetime = None) -> Optional[Dict]:
        """Get aggregated values for a window."""
        timestamp = timestamp or datetime.utcnow()
        window_start = self._get_window_start(timestamp)
        window_key = f"{key}:{window_start}"
        
        with self.lock:
            window = self.windows.get(window_key)
            if window:
                return {
                    "key": key,
                    "window_start": datetime.fromtimestamp(window["window_start"]),
                    "count": window["count"],
                    "sum": window["sum"],
                    "avg": window["sum"] / window["count"] if window["count"] > 0 else 0,
                    "values": window["values"],
                }
        return None
    
    def flush_expired(self, max_age_seconds: int = 300):
        """Remove expired windows."""
        cutoff = datetime.utcnow().timestamp() - max_age_seconds
        
        with self.lock:
            expired = [
                k for k, v in self.windows.items()
                if v["window_start"] < cutoff
            ]
            for k in expired:
                del self.windows[k]
        
        return len(expired)
