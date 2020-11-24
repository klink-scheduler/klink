# Klink v0.1
#### A Progress-Aware Low-Latency Query Scheduler for Flink

Klink is an advanced query scheduler for stream processing systems. Klink optimizes for stream progress to reduce the output latency for queries running window operators, including joins.

Klink has the following characteristics:
* Leverages watermarks to monitor stream progress and prioritize the execution of queries that include window operators that are due to be processed first.
* Includes a memory management module that reduces memory stress when appropriate.
* Supports both tuple-by-tuple and micro-batching processing models
* Supports distributed deployments 

This project repository hosts a prototype implementation of Klink for the Apache Flink stream processing system [https://flink.apache.org].

Similarly to previous works (cfr. Aurora and Borealis) Klink is implemented as a state-based  runtime scheduler within Flink. Flink is divided into two main integral components: Runtime and streaming-layer. Klink modified the two layers to implement a Runtime scheduler.

### Runtime layer
The changes on the runtime layer entail registering all Tasks (or `AbstractInvokable`) with a newly built component, that is, the `RuntimeScheduler`. Specifically, under `flink-runtime/taskexecutor/scheduler`, one can find a new module built for a runtime scheduler. 

The scheduler, initially launched before any other tasks, and through carefully curating it to avoid race conditions, intercepts the creation of other Tasks. Each task is formally registered, which allows us to toggle control of each thread, as well as collect the necessary runtime information. The scheduler is then converted into a streaming scheduler in the streaming layer. Note that the design is extendable to support batching.

### Streaming layer
The streaming layer scheduler can be found in `flink-streaming-java/streaming/runtime/tasks/scheduler`. The scheduler is invoked by the Runtime scheduler. Specifically, all Tasks registered with the runtime scheduler are sent to the streaming scheduler that converts the `AbstractInvokables` to their respective type of operators. The type of the scheduler is then specified when specifying the query by the end user. The factory then creates the right instance of the scheduler.

Klink scheduler is found in the algorithm module, attached with the `DistStore`, which holds information about the network delay as well as the inter-event generation delays.

## Installation
### Unix
- Requirements-like environment (we use Linux, Mac OS X, Cygwin, WSL)
- Git
- Maven (we recommend version 3.2.5 and require at least 3.1.1)
- Java 8 or 11 (Java 9 or 10 may work)
- Flink v1.8

### Setup and execution
- Replace the provided Klink modules (flink-runtime and flink-streaming-java) of that of the original Flink v1.8.
- Set it up as normal Flink instance.

## License
The Klink scheduler is licensed under the terms of the Apache License 2.0 license and is available for free.
