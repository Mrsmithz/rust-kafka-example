# Open issues

These issues are tracked in this repository because no authenticated forge CLI is available in the implementation environment.

## KAFKA-001 Native commit generation fencing

Status is open. This is a production acceptance gap in the original strict ownership-fencing requirement.

Assignment tokens reject known stale processing results before commit submission and after confirmation. Those checks are not atomic with native commit submission. The pinned Rust API accepts explicit offsets but does not take the application's ownership epoch.

A native synchronous commit runs concurrently with polling so that commit waits cannot stall group callbacks. The remaining race is between the application's epoch check and the native client's selection of group membership metadata.

Closure requires a reproducible broker test with delayed commit submission and same-partition reassignment. The test must identify the generation attached to the actual native request. It must show either that stale submission is excluded or that a different native API or adapter is needed.

A passing unit test for token cancellation is not sufficient evidence. Until closure, the service is an at-least-once baseline with duplicate risk, not a strict native-generation-fencing certification.
