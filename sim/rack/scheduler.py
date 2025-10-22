#!/usr/bin/env python
"""Central scheduler that tracks worker capability based on requeue frequency."""

import logging
from collections import defaultdict, deque
from worker import Worker
import time

class RackScheduler:
    """
    Rack scheduler that uses window-based scoring to track worker performance
    and make scheduling decisions based on queue length and preemption history.
    """
    def __init__(self, workers, config, state):
        self.workers = workers
        self.config = config
        self.state = state

        self.worker_capability_scores = defaultdict(float)  # worker_id -> capability score (0-1)
        self.worker_stability = defaultdict(float)  # worker_id -> stability metric
        self.stability_decay_factor = config.STABILITY_DECAY_FACTOR if hasattr(config, 'STABILITY_DECAY_FACTOR') else 0.95
        self.capability_weight = config.CAPABILITY_WEIGHT if hasattr(config, 'CAPABILITY_WEIGHT') else 0.7

        self.WINDOW_SIZE = 5000
        
        # Initialize per-worker metrics
        self.worker_metrics = defaultdict(lambda: {
            'preemption_times': deque(),  # Times of preemption events within window
            'last_update': 0,  # Last update timestamp
            # 'current_score': 0.0  # Current computed score
        })
        
        # Initialize metrics for all workers
        for worker in self.workers:
            self.worker_metrics[worker.identifier]['last_update'] = self.state.timer.get_time()

    def update_worker_capability(self):
        """Update capability scores for all workers based on their current status."""
        current_time = self.state.timer.get_time()

        for worker in self.workers:
            metrics = self.worker_metrics[worker.identifier]
            # Get current queue length
            current_queue_length = worker.queue.length() + int(worker.is_busy())
            
            # Get current preemption count from worker
            current_preemptions = worker.preemption_counts
            if current_preemptions > metrics.get('last_preemption_count', 0):
                # New preemptions occurred
                new_preemptions = current_preemptions - metrics.get('last_preemption_count', 0)
                for _ in range(new_preemptions):
                    metrics['preemption_times'].append(current_time)
            
            # Update last known preemption count
            metrics['last_preemption_count'] = current_preemptions
            
            # Clean up old preemption records
            while (metrics['preemption_times'] and 
                   current_time - metrics['preemption_times'][0] > self.WINDOW_SIZE):
                metrics['preemption_times'].popleft()
            
            # Count preemptions within the window
            recent_preemptions = len(metrics['preemption_times'])
            metrics['last_update'] = current_time

            # # Calculate score dynamically
            # score = (
            #     current_queue_length * self.config.AVERAGE_SERVICE_TIME + 
            #     (recent_preemptions / self.WINDOW_SIZE) * self.WINDOW_SIZE * 2
            # )

            # logging.debug(f"[SCHEDULER] Worker {worker.identifier} dynamic score calculated: {score:.2f}")

        # logging.debug(f"[SCHEDULER] Worker capability scores: {dict(self.worker_capability_scores)}")

    def get_best_worker_for_task(self, workers=None):
        """Get the worker with the best score (lowest delay estimate)."""
        if not workers:
            return 0
            
        best_worker_id = 0
        best_score = float('inf')
        
        # Find worker with best composite score (lowest delay estimate)
        for worker in workers:
            metrics = self.worker_metrics[worker.identifier]
            
            # Composite score: balances capability score and queue length
            # Lower score is better
            # Include remaining preemption timer time in the score calculation
            preemption_time_factor = metrics.get('preemption_time_remaining', float('inf'))
            score = (
                len(metrics['preemption_times']) * self.capability_weight + 
                (worker.queue.length() + int(worker.is_busy())) * (1 - self.capability_weight) +
                (self.config.PREEMPTION_TIME - worker.preemption_timer.get_time()) / self.config.PREEMPTION_TIME
            ) if worker.queue.length() + int(worker.is_busy()) > 0 else 0
            
            logging.debug(f"[SCHEDULER]: Worker {worker.identifier} - "
                         f"Remaining Preemption Time: {self.config.PREEMPTION_TIME - worker.preemption_timer.get_time():.3f}, "
                         f"Preemptions in window: {len(metrics['preemption_times'])}, "
                         f"Queue length: {worker.queue.length() + int(worker.is_busy())}, "
                         f"Composite score: {score:.3f}")
            
            if score < best_score:
                best_score = score
                best_worker_id = worker.identifier
        
        logging.debug(f"[SCHEDULER]: Selected worker {best_worker_id} with composite score {best_score:.3f}")
        return best_worker_id
    
    def get_shortest_queue(self, workers=None):
        """Get the queue with the shortest estimated delay."""
        if not workers:
            workers = self.workers

        if self.config.join_shortest_estimated_delay_queue:
            # Use our window-based scoring system
            best_worker_id = self.get_best_worker_for_task(workers)
            logging.debug(f"[SCHEDULER]: Best worker ID for task: {best_worker_id}")
            return best_worker_id
            
        elif self.config.join_shortest_queue:
            # Simple shortest queue logic
            return min(workers, key=lambda w: w.queue.length() + int(w.is_busy())).identifier
            
        elif self.config.global_queue:
            # For global queue, return any worker (will be handled by global queue)
            return workers[0].identifier

    # def update_worker_capability(self):
    #     """Update the capability score for a worker based on requeue history."""
    #     for worker in self.workers:
    #         worker_id = worker.identifier
    #         recent_requeues = self.state.workers[worker_id].recent_requeue_counts

    #         # Base capability starts at 1.0 and decreases with more requeues
    #         if not recent_requeues:
    #             capability = 1.0
    #         else:
    #             # Exponential decay based on number of recent requeues
    #             capability = max(0.1, 1.0 / (1.0 + recent_requeues * 0.5))
            
    #         # Apply stability factor (workers that consistently perform well maintain higher scores)
    #         capability *= self.worker_stability[worker_id]
            
    #         # Apply decay to stability over time
    #         self.worker_stability[worker_id] *= self.stability_decay_factor
            
    #         self.worker_capability_scores[worker_id] = capability

    def count_preemptions_within_window(self, worker, time_window):
        """Count preemptions for a worker within the given time window."""
        return worker.get_preemptions_within_window(time_window)

    def schedule(self):
        """Schedule tasks considering preemptions within a time window."""
        current_time = self.state.timer.get_time()
        for worker in self.workers:
            preemptions_in_window = self.count_preemptions_within_window(worker, self.WINDOW_SIZE)
            logging.debug(f"[SCHEDULER]: Worker {worker.identifier} has {preemptions_in_window} preemptions in the last {self.WINDOW_SIZE}ms.")

# class CentralScheduler:
#     """
#     Central scheduler that tracks requeue times at each worker and makes 
#     intelligent scheduling decisions based on worker capability.
#     """
    
#     def __init__(self, config, state):
#         self.config = config
#         self.state = state
        
#         # # Track requeue events per worker
#         # self.requeue_history = defaultdict(deque)  # worker_id -> deque of requeue timestamps
#         # self.preemption_counts = defaultdict(int)  # worker_id -> count of preemptions
#         # self.recent_requeue_counts = defaultdict(int)  # worker_id -> recent requeue count
        
#         # Capability metrics
#         self.worker_capability_scores = defaultdict(float)  # worker_id -> capability score (0-1)
#         self.worker_stability = defaultdict(float)  # worker_id -> stability metric
        
#         # Configuration parameters
#         self.requeue_history_window = config.REQUEUE_HISTORY_WINDOW if hasattr(config, 'REQUEUE_HISTORY_WINDOW') else 5000  # Time window to consider
#         self.stability_decay_factor = config.STABILITY_DECAY_FACTOR if hasattr(config, 'STABILITY_DECAY_FACTOR') else 0.95
#         self.capability_weight = config.CAPABILITY_WEIGHT if hasattr(config, 'CAPABILITY_WEIGHT') else 0.7
        
#         # Initialize all workers with perfect capability
#         for worker in state.workers:
#             self.worker_capability_scores[worker.identifier] = 1.0
#             self.worker_stability[worker.identifier] = 1.0
    
#     def record_requeue(self, worker_id, task):
#         """Record a requeue event for a worker."""
#         current_time = self.state.timer.get_time()
        
#         # Add to requeue history
#         self.requeue_history[worker_id].append(current_time)
#         self.preemption_counts[worker_id] += 1
        
#         # Clean old history outside the window
#         while (self.requeue_history[worker_id] and 
#                current_time - self.requeue_history[worker_id][0] > self.requeue_history_window):
#             self.requeue_history[worker_id].popleft()
        
#         # Update recent requeue count
#         self.recent_requeue_counts[worker_id] = len(self.requeue_history[worker_id])
        
#         # Update capability score
#         self._update_worker_capability(worker_id)
        
#         logging.debug(f"[REQUEUE]: Worker {worker_id} requeued task {task}, "
#                      f"recent requeues: {self.recent_requeue_counts[worker_id]}, "
#                      f"capability: {self.worker_capability_scores[worker_id]:.3f}")
    
#     def record_task_completion(self, worker_id, task):
#         """Record a successful task completion."""
#         # Boost capability score slightly for successful completion
#         self.worker_stability[worker_id] = min(1.0, 
#             self.worker_stability[worker_id] * 1.001 + 0.001)
#         self._update_worker_capability(worker_id)
    
#     def _update_worker_capability(self, worker_id):
#         """Update the capability score for a worker based on requeue history."""
#         recent_requeues = self.recent_requeue_counts[worker_id]
        
#         # Base capability starts at 1.0 and decreases with more requeues
#         if recent_requeues == 0:
#             capability = 1.0
#         else:
#             # Exponential decay based on number of recent requeues
#             capability = max(0.1, 1.0 / (1.0 + recent_requeues * 0.5))
        
#         # Apply stability factor (workers that consistently perform well maintain higher scores)
#         capability *= self.worker_stability[worker_id]
        
#         # Apply decay to stability over time
#         self.worker_stability[worker_id] *= self.stability_decay_factor
        
#         self.worker_capability_scores[worker_id] = capability
    
#     def get_best_worker_for_task(self, task=None):
#         """
#         Get the best worker to assign a task to, considering both queue length 
#         and worker capability.
#         """
#         if not self.state.workers:
#             return 0
        
#         best_worker_id = 0
#         best_score = float('-inf')
        
#         for worker in self.state.workers:
#             worker_id = worker.identifier
#             queue = worker.queue
            
#             # Get queue length
#             queue_length = queue.length() + int(worker.is_busy()) if queue else 0
            
#             # Get worker capability
#             capability = self.worker_capability_scores[worker_id]
            
#             # Calculate composite score:
#             # Higher capability is better (positive), shorter queue is better (negative queue length)
#             # We want to balance between capability and queue length
#             queue_penalty = queue_length * 2.0  # Weight queue length more heavily
#             capability_bonus = capability * 10.0  # Scale capability appropriately
            
#             composite_score = capability_bonus - queue_penalty
            
#             logging.debug(f"[SCHEDULER]: Worker {worker_id} - queue_len: {queue_length}, preemptions: {self.preemption_counts[worker_id]}, "
#                          f"capability: {capability:.3f}, score: {composite_score:.3f}")
            
#             if composite_score > best_score:
#                 best_score = composite_score
#                 best_worker_id = worker_id
        
#         logging.debug(f"[SCHEDULER]: Selected worker {best_worker_id} with score {best_score:.3f}")
#         return best_worker_id
    
#     def get_shortest_capable_queue(self):
#         """
#         Enhanced version of get_shortest_queue that considers worker capability.
#         Returns the queue index that represents the best balance of queue length and worker capability.
        
#         logging.debug(f"[SCHEDULER]: Selected worker {best_worker_id} with score {best_score:.3f}")
#         return best_worker_id
    
#     def get_shortest_capable_queue(self):
#         """
#         Enhanced version of get_shortest_queue that considers worker capability.
#         Returns the queue index that represents the best balance of queue length and worker capability.
#         """
#         if not self.state.queues:
#             return 0
            
#         # Find the worker ID with the best score
#         best_worker_id = self.get_best_worker_for_task()
        
#         # Map worker ID to queue index using the mapping
#         if best_worker_id < len(self.config.mapping):
#             return self.config.mapping[best_worker_id]
        
#         # Fallback to original shortest queue logic
#         return min(range(len(self.state.queues)), key=lambda i: self.state.queues[i].length())
    
#     def get_shortest_queue_simple(self):
#         """
#         Original shortest queue logic - just returns the queue with minimum length.
#         Used when join_shortest_queue=True but join_shortest_estimated_delay_queue=False.
#         """
#         if not self.state.queues:
#             return 0
#         return min(range(len(self.state.queues)), key=lambda i: self.state.queues[i].length())
    
#     def should_preempt_for_capability(self, worker_id, incoming_task_priority=False):
#         """
#         Determine if a worker should preempt its current task based on capability considerations.
#         """
#         if worker_id not in self.worker_capability_scores:
#             return False
        
#         capability = self.worker_capability_scores[worker_id]
#         recent_requeues = self.recent_requeue_counts[worker_id]
        
#         # If worker has low capability and many recent requeues, 
#         # be more conservative about preemption
#         if capability < 0.5 and recent_requeues > 3:
#             return False
        
#         # For high priority incoming tasks, allow preemption even on struggling workers
#         if incoming_task_priority:
#             return True
        
#         # Normal preemption logic based on capability
#         return capability > 0.7
    
#     def get_worker_stats(self):
#         """Get statistics about worker capabilities for monitoring."""
#         stats = {}
#         for worker_id in self.worker_capability_scores:
#             stats[worker_id] = {
#                 'capability_score': self.worker_capability_scores[worker_id],
#                 'stability': self.worker_stability[worker_id],
#                 'recent_requeues': self.recent_requeue_counts[worker_id],
#                 'total_preemptions': self.preemption_counts[worker_id]
#             }
#         return stats
    
#     def log_worker_status(self):
#         """Log current worker status for debugging."""
#         for worker_id in self.worker_capability_scores:
#             logging.info(f"[WORKER_STATUS]: Worker {worker_id} - "
#                         f"Capability: {self.worker_capability_scores[worker_id]:.3f}, "
#                         f"Stability: {self.worker_stability[worker_id]:.3f}, "
#                         f"Recent requeues: {self.recent_requeue_counts[worker_id]}, "
#                         f"Total preemptions: {self.preemption_counts[worker_id]}")