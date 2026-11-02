"""Ownership errors for database-backed worker jobs."""


class JobLeaseLost(RuntimeError):
    """The attempt no longer owns the job and cannot update its state."""
