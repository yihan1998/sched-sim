#!/usr/bin/env python
"""Central scheduler that tracks worker capability based on requeue frequency."""

import logging
from collections import defaultdict, deque
import time


class CentralScheduler:
    """
    Central scheduler that tracks requeue times at each worker and makes 
    intelligent scheduling decisions based on worker capability.
    """
    
    def __init__(self, config, state):
        self.config = config
        self.state = state
        
        # Track requeue events per worker
        self.requeue_history = defaultdict(deque)  # worker_id -> deque of requeue timestamps
        self.preemption_counts = defaultdict(int)  # worker_id -> count of preemptions
        self.recent_requeue_counts = defaultdict(int)  # worker_id -> recent requeue count
        
        # Capability metrics
        self.worker_capability_scores = defaultdict(float)  # worker_id -> capability score (0-1)
        self.worker_stability = defaultdict(float)  # worker_id -> stability metric
        
        # Configuration parameters
        self.requeue_history_window = config.REQUEUE_HISTORY_WINDOW if hasattr(config, 'REQUEUE_HISTORY_WINDOW') else 5000  # Time window to consider
        self.stability_decay_factor = config.STABILITY_DECAY_FACTOR if hasattr(config, 'STABILITY_DECAY_FACTOR') else 0.95
        self.capability_weight = config.CAPABILITY_WEIGHT if hasattr(config, 'CAPABILITY_WEIGHT') else 0.7
        
        # Initialize all workers with perfect capability
        for worker in state.workers:
            self.worker_capability_scores[worker.id] = 1.0
            self.worker_stability[worker.id] = 1.0
    
    def record_requeue(self, worker_id, task):
        """Record a requeue event for a worker."""
        current_time = self.state.timer.get_time()
        
        # Add to requeue history
        self.requeue_history[worker_id].append(current_time)
        self.preemption_counts[worker_id] += 1
        
        # Clean old history outside the window
        while (self.requeue_history[worker_id] and 
               current_time - self.requeue_history[worker_id][0] > self.requeue_history_window):
            self.requeue_history[worker_id].popleft()
        
        # Update recent requeue count
        self.recent_requeue_counts[worker_id] = len(self.requeue_history[worker_id])
        
        # Update capability score
        self._update_worker_capability(worker_id)
        
        logging.debug(f"[REQUEUE]: Worker {worker_id} requeued task {task}, "
                     f"recent requeues: {self.recent_requeue_counts[worker_id]}, "
                     f"capability: {self.worker_capability_scores[worker_id]:.3f}")
    
    def record_task_completion(self, worker_id, task):
        """Record a successful task completion."""
        # Boost capability score slightly for successful completion
        self.worker_stability[worker_id] = min(1.0, 
            self.worker_stability[worker_id] * 1.001 + 0.001)
        self._update_worker_capability(worker_id)
    
    def _update_worker_capability(self, worker_id):
        """Update the capability score for a worker based on requeue history."""
        recent_requeues = self.recent_requeue_counts[worker_id]
        
        # Base capability starts at 1.0 and decreases with more requeues
        if recent_requeues == 0:
            capability = 1.0
        else:
            # Exponential decay based on number of recent requeues
            capability = max(0.1, 1.0 / (1.0 + recent_requeues * 0.5))
        
        # Apply stability factor (workers that consistently perform well maintain higher scores)
        capability *= self.worker_stability[worker_id]
        
        # Apply decay to stability over time
        self.worker_stability[worker_id] *= self.stability_decay_factor
        
        self.worker_capability_scores[worker_id] = capability
    
    def get_best_worker_for_task(self, task=None):
        """
        Get the best worker to assign a task to, considering both queue length 
        and worker capability.
        """
        if not self.state.workers:
            return 0
        
        best_worker_id = 0
        best_score = float('-inf')
        
        for worker in self.state.workers:
            worker_id = worker.id
            queue = worker.queue
            
            # Get queue length
            queue_length = queue.length() if queue else 0
            
            # Get worker capability
            capability = self.worker_capability_scores[worker_id]
            
            # Calculate composite score:
            # Higher capability is better (positive), shorter queue is better (negative queue length)
            # We want to balance between capability and queue length
            queue_penalty = queue_length * 2.0  # Weight queue length more heavily
            capability_bonus = capability * 10.0  # Scale capability appropriately
            
            composite_score = capability_bonus - queue_penalty
            
            logging.debug(f"[SCHEDULER]: Worker {worker_id} - queue_len: {queue_length}, "
                         f"capability: {capability:.3f}, score: {composite_score:.3f}")
            
            if composite_score > best_score:
                best_score = composite_score
                best_worker_id = worker_id
        
        logging.debug(f"[SCHEDULER]: Selected worker {best_worker_id} with score {best_score:.3f}")
        return best_worker_id
    
    def get_shortest_capable_queue(self):
        """
        Enhanced version of get_shortest_queue that considers worker capability.
        Returns the queue index that represents the best balance of queue length and worker capability.
        """
        if not self.state.queues:
            return 0
            
        # Find the worker ID with the best score
        best_worker_id = self.get_best_worker_for_task()
        
        # Map worker ID to queue index using the mapping
        if best_worker_id < len(self.config.mapping):
            return self.config.mapping[best_worker_id]
        
        # Fallback to original shortest queue logic
        return min(range(len(self.state.queues)), key=lambda i: self.state.queues[i].length())
    
    def get_shortest_queue_simple(self):
        """
        Original shortest queue logic - just returns the queue with minimum length.
        Used when join_shortest_queue=True but join_shortest_queue_by_capacity=False.
        """
        if not self.state.queues:
            return 0
        return min(range(len(self.state.queues)), key=lambda i: self.state.queues[i].length())
    
    def should_preempt_for_capability(self, worker_id, incoming_task_priority=False):
        """
        Determine if a worker should preempt its current task based on capability considerations.
        """
        if worker_id not in self.worker_capability_scores:
            return False
        
        capability = self.worker_capability_scores[worker_id]
        recent_requeues = self.recent_requeue_counts[worker_id]
        
        # If worker has low capability and many recent requeues, 
        # be more conservative about preemption
        if capability < 0.5 and recent_requeues > 3:
            return False
        
        # For high priority incoming tasks, allow preemption even on struggling workers
        if incoming_task_priority:
            return True
        
        # Normal preemption logic based on capability
        return capability > 0.7
    
    def get_worker_stats(self):
        """Get statistics about worker capabilities for monitoring."""
        stats = {}
        for worker_id in self.worker_capability_scores:
            stats[worker_id] = {
                'capability_score': self.worker_capability_scores[worker_id],
                'stability': self.worker_stability[worker_id],
                'recent_requeues': self.recent_requeue_counts[worker_id],
                'total_preemptions': self.preemption_counts[worker_id]
            }
        return stats
    
    def log_worker_status(self):
        """Log current worker status for debugging."""
        for worker_id in self.worker_capability_scores:
            logging.info(f"[WORKER_STATUS]: Worker {worker_id} - "
                        f"Capability: {self.worker_capability_scores[worker_id]:.3f}, "
                        f"Stability: {self.worker_stability[worker_id]:.3f}, "
                        f"Recent requeues: {self.recent_requeue_counts[worker_id]}, "
                        f"Total preemptions: {self.preemption_counts[worker_id]}")