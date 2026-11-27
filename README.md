# Trinity Federated Network

Multi-node sense-making. Each node runs its own chamber with a distinct model mix. Programs cross the network; selection pressure is which promotions peers adopt.

## Run

```bash
cd backend && npm install && MOCK=1 PORT=4202 NODE_ID=node-alpha npm run dev
# second terminal
MOCK=1 PORT=4203 NODE_ID=node-beta npm run dev
curl -X POST localhost:4202/api/federation/peers -H 'content-type: application/json' -d '{"name":"beta","url":"http://localhost:4203"}'
curl -X POST localhost:4202/api/federation/sync
```

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/federation/status` | Node id, peers, inbox |
| POST | `/api/federation/sync` | Pull programs from peers |
| POST | `/api/federation/broadcast` | Push local programs |
| POST | `/api/federation/resolve` | Conflict resolution |
| GET | `/api/federation/adoption` | Adoption rate |
| POST | `/api/chamber/run` | Local chamber |
