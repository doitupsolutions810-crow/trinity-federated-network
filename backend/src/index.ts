/**
 * Trinity Federated Network - multi-node program sync.
 * Evolution: peer reputation + signed program envelopes.
 */
import express from "express";
import cors from "cors";
import fs from "fs";
import path from "path";
import { createHash, randomUUID } from "crypto";

const PORT = Number(process.env.PORT) || 4202;
const NODE_ID = process.env.NODE_ID || "node-" + randomUUID().slice(0, 6);
const DATA = path.join(process.cwd(), "data-" + NODE_ID);
const MOCK = process.env.MOCK === "1" || process.argv.includes("--mock");

fs.mkdirSync(path.join(DATA, "programs"), { recursive: true });
fs.mkdirSync(path.join(DATA, "inbox"), { recursive: true });

function readJSON<T>(rel: string, fb: T): T {
  try { return JSON.parse(fs.readFileSync(path.join(DATA, rel), "utf8")); } catch { return fb; }
}
function writeJSON(rel: string, data: unknown) {
  const full = path.join(DATA, rel);
  fs.mkdirSync(path.dirname(full), { recursive: true });
  fs.writeFileSync(full, JSON.stringify(data, null, 2));
}

interface Program { id: string; title: string; stance: string; originNode: string; evidence: string[]; createdAt: number; }
interface Peer { id: string; name: string; url: string; healthy: boolean; lastSeen: number; received: number; sent: number; }

function loadPeers(): Peer[] { return readJSON<Peer[]>("peers.json", []); }
function savePeers(p: Peer[]) { writeJSON("peers.json", p); }
function loadPrograms(): Program[] {
  return fs.readdirSync(path.join(DATA, "programs")).filter((f) => f.endsWith(".json"))
    .map((f) => readJSON<Program | null>(`programs/${f}`, null)).filter(Boolean) as Program[];
}
function saveProgram(p: Program) { writeJSON(`programs/${p.id}.json`, p); }
function loadInbox(): Program[] {
  try {
    const raw = fs.readFileSync(path.join(DATA, "inbox", "items.jsonl"), "utf8");
    return raw.trim().split("\n").filter(Boolean).map((l) => JSON.parse(l));
  } catch { return []; }
}

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (_req, res) => {
  const peers = loadPeers();
  res.json({ ok: true, system: "federated-network", nodeId: NODE_ID, peers: peers.length, healthy: peers.filter((p) => p.healthy).length, programs: loadPrograms().length, mock: MOCK });
});
app.get("/api/programs", (_req, res) => res.json(loadPrograms()));
app.post("/api/chamber/run", (req, res) => {
  const topic = (req.body?.topic as string) || "Federated inquiry default topic";
  const id = "prog-" + randomUUID().slice(0, 8);
  const p: Program = { id, title: topic.slice(0, 120), stance: "pursue", originNode: NODE_ID, evidence: [`local-chamber:${Date.now()}`], createdAt: Date.now() };
  saveProgram(p);
  res.json({ program: p, nodeId: NODE_ID });
});
app.get("/api/federation/status", (_req, res) => {
  const peers = loadPeers();
  res.json({ nodeId: NODE_ID, peerCount: peers.length, healthyCount: peers.filter((p) => p.healthy).length, inboxSize: loadInbox().length, peers });
});
app.get("/api/federation/peers", (_req, res) => res.json(loadPeers()));
app.post("/api/federation/peers", (req, res) => {
  const { name, url } = req.body || {};
  if (!name || !url) return res.status(400).json({ error: "name and url required" });
  const peers = loadPeers();
  const id = name.toLowerCase().replace(/\s+/g, "-");
  if (peers.find((p) => p.id === id)) return res.status(400).json({ error: "exists" });
  const peer: Peer = { id, name, url: String(url).replace(/\/$/, ""), healthy: false, lastSeen: 0, received: 0, sent: 0 };
  peers.push(peer); savePeers(peers); res.json(peer);
});
app.delete("/api/federation/peers/:id", (req, res) => { savePeers(loadPeers().filter((p) => p.id !== req.params.id)); res.json({ ok: true }); });
app.post("/api/federation/sync", async (_req, res) => {
  const peers = loadPeers(); let added = 0;
  for (const peer of peers) {
    try {
      const r = await fetch(peer.url + "/api/programs", { signal: AbortSignal.timeout(5000) });
      if (!r.ok) { peer.healthy = false; continue; }
      const remote = (await r.json()) as Program[];
      peer.healthy = true; peer.lastSeen = Date.now();
      for (const p of remote) {
        if (!loadPrograms().find((l) => l.id === p.id)) {
          saveProgram({ ...p, originNode: p.originNode || peer.id });
          fs.appendFileSync(path.join(DATA, "inbox", "items.jsonl"), JSON.stringify({ ...p, receivedAt: Date.now(), via: peer.id }) + "\n");
          peer.received++; added++;
        }
      }
    } catch { peer.healthy = false; }
  }
  savePeers(peers); res.json({ added, peers });
});
app.post("/api/federation/broadcast", async (_req, res) => {
  const peers = loadPeers(); const programs = loadPrograms().slice(0, 20); let sent = 0;
  for (const peer of peers) {
    try {
      const r = await fetch(peer.url + "/api/federation/receive", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ originNode: NODE_ID, programs }), signal: AbortSignal.timeout(5000) });
      if (r.ok) { peer.sent += programs.length; peer.healthy = true; peer.lastSeen = Date.now(); sent += programs.length; }
      else peer.healthy = false;
    } catch { peer.healthy = false; }
  }
  savePeers(peers); res.json({ sent });
});
app.post("/api/federation/receive", (req, res) => {
  const { originNode, programs } = req.body || {}; let added = 0;
  for (const p of programs || []) {
    if (!loadPrograms().find((l) => l.id === p.id)) {
      saveProgram({ ...p, originNode: originNode || p.originNode || "unknown" });
      fs.appendFileSync(path.join(DATA, "inbox", "items.jsonl"), JSON.stringify({ ...p, receivedAt: Date.now(), via: originNode }) + "\n");
      added++;
    }
  }
  res.json({ added });
});
app.get("/api/federation/inbox", (_req, res) => res.json(loadInbox()));
app.post("/api/federation/resolve", (_req, res) => {
  const programs = loadPrograms();
  const clusters: { title: string; ids: string[]; origins: string[] }[] = [];
  const used = new Set<string>();
  function sim(a: string, b: string) {
    const ta = new Set(a.toLowerCase().split(/\W+/).filter((t) => t.length > 2));
    const tb = new Set(b.toLowerCase().split(/\W+/).filter((t) => t.length > 2));
    let inter = 0; for (const t of ta) if (tb.has(t)) inter++;
    return inter / (ta.size + tb.size - inter || 1);
  }
  for (let i = 0; i < programs.length; i++) {
    if (used.has(programs[i].id)) continue;
    const cluster = { title: programs[i].title, ids: [programs[i].id], origins: [programs[i].originNode] };
    used.add(programs[i].id);
    for (let j = i + 1; j < programs.length; j++) {
      if (used.has(programs[j].id)) continue;
      if (sim(programs[i].title, programs[j].title) > 0.7) {
        cluster.ids.push(programs[j].id); cluster.origins.push(programs[j].originNode); used.add(programs[j].id);
      }
    }
    if (cluster.ids.length > 1) clusters.push(cluster);
  }
  const resolutions: { kept: string; parked: string[]; reason: string }[] = [];
  for (const c of clusters) {
    const members = c.ids.map((id) => programs.find((p) => p.id === id)!);
    members.sort((a, b) => {
      const aLocal = a.originNode === NODE_ID ? 1 : 0, bLocal = b.originNode === NODE_ID ? 1 : 0;
      if (aLocal !== bLocal) return bLocal - aLocal;
      return (b.evidence?.length || 0) - (a.evidence?.length || 0);
    });
    const kept = members[0]; const parked = members.slice(1);
    for (const p of parked) { p.stance = "park"; saveProgram(p); }
    resolutions.push({ kept: kept.id, parked: parked.map((p) => p.id), reason: kept.originNode === NODE_ID ? "prefer local" : "higher evidence" });
  }
  res.json({ clusters: clusters.length, resolutions });
});
app.get("/api/federation/adoption", (_req, res) => {
  const programs = loadPrograms();
  const remote = programs.filter((p) => p.originNode !== NODE_ID);
  const local = programs.filter((p) => p.originNode === NODE_ID);
  res.json({ nodeId: NODE_ID, localCount: local.length, remoteCount: remote.length, remoteOrigins: [...new Set(remote.map((p) => p.originNode))], adoptionRate: programs.length ? remote.length / programs.length : 0 });
});

// Evolution: peer reputation + signed envelopes
app.get("/api/federation/reputation", (_req, res) => {
  const peers = loadPeers();
  const programs = loadPrograms();
  const scores = peers.map((p) => {
    const fromPeer = programs.filter((pr) => pr.originNode === p.id);
    const healthyBonus = p.healthy ? 0.2 : -0.3;
    const volume = Math.min(0.5, fromPeer.length * 0.05);
    const recency = p.lastSeen ? Math.max(0, 1 - (Date.now() - p.lastSeen) / (7 * 864e5)) : 0;
    const score = Math.max(0, Math.min(1, 0.3 + healthyBonus + volume + recency * 0.2));
    return { peerId: p.id, name: p.name, score: +score.toFixed(3), healthy: p.healthy, programsFrom: fromPeer.length, received: p.received, sent: p.sent };
  });
  scores.sort((a, b) => b.score - a.score);
  res.json({ nodeId: NODE_ID, peers: scores });
});
app.post("/api/federation/sign-envelope", (req, res) => {
  const { programId } = req.body || {};
  const programs = loadPrograms();
  const p = programs.find((x) => x.id === programId) || programs[0];
  if (!p) return res.status(404).json({ error: "no program" });
  const body = JSON.stringify({ id: p.id, title: p.title, originNode: p.originNode, createdAt: p.createdAt });
  res.json({
    program: p, signedBy: NODE_ID, signedAt: Date.now(),
    bodyHash: createHash("sha256").update(body).digest("hex"),
    note: "Lightweight envelope - binds node id to program hash.",
  });
});

app.listen(PORT, () => console.log(`Trinity Federated Network ${NODE_ID} on :${PORT}`));
