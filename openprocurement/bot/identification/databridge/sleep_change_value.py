class APIRateController:
    def __init__(self, increment_step=1, decrement_step=1):
        self.increment_step = increment_step
        self.decrement_step = decrement_step
        self.time_between_requests = 0

    def decrement(self):
        self.time_between_requests -= self.decrement_step if self.decrement_step < self.time_between_requests else 0
        return self.time_between_requests

    def increment(self):
        self.time_between_requests += self.increment_step
        return self.time_between_requests