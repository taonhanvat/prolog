# Prolog - Distributed Log Service

A high-performance, fault-tolerant distributed log service built in Go, implementing Raft consensus for strong consistency and Serf for cluster membership management.

## Architecture

Prolog is designed as a distributed system with the following key components:

- **gRPC API**: Client-facing service for log operations
- **Raft Consensus**: Ensures data consistency across cluster nodes
- **Serf Membership**: Handles node discovery and cluster management
- **Segmented Storage**: Efficient log storage with indexing
- **Access Control**: Casbin-based authorization system

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client API    │    │   Client API    │    │   Client API    │
│   (Port 8400)   │    │   (Port 8400)   │    │   (Port 8400)   │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Membership/Serf │    │ Membership/Serf │    │ Membership/Serf │
│   (Port 8401)   │◄──►│   (Port 8401)   │◄──►│   (Port 8401)   │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│  Raft Consensus │    │  Raft Consensus │    │  Raft Consensus │
│   (Port 8402)   │◄──►│   (Port 8402)   │◄──►│   (Port 8402)   │
├─────────────────┤    ├─────────────────┤    ├─────────────────┤
│ Segmented Store │    │ Segmented Store │    │ Segmented Store │
└─────────────────┘    └─────────────────┘    └─────────────────┘
     Leader                Follower              Follower
```