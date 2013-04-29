dist.go
=======

A simple work distributor

Given a Job with n parallel steps, perform the steps in parallel and wait for results.
The parallel steps are run on seperate servers using a simple binary protocol.

<length of json payload:2 bytes><json encoded data>

Jobs are configured by config file: services.yaml

Roadmap:
1. Get something working [done]
2. Deal with errors dont die
3. Add connection pooling
4. Add http frontend
4. Benchmarks
