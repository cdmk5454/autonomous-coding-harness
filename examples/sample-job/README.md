# Synthetic Job lifecycle

The Job freezes its profile, scope, Worker route, operation capabilities, and deterministic test plan. The Worker writes only within the declared candidate scope. Build and Test evidence is bound to that candidate; Review and Verification consume the same candidate identity and evidence hashes. A failed attempt preserves the candidate and any previously accepted batch delta for fenced recovery.
