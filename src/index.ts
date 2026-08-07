import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// ─────────────────────────────────────────────
// Durable Object: manages WebSocket connections
// and broadcasts new feedback to all displays.
// ─────────────────────────────────────────────
export class FeedbackRoom extends DurableObject {
	private sessions: Set<WebSocket> = new Set();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		// Ensure the feedback table exists (runs once per DO instance)
		this.ctx.storage.sql.exec(`
      CREATE TABLE IF NOT EXISTS feedback (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        text       TEXT    NOT NULL,
        created_at INTEGER NOT NULL DEFAULT (unixepoch())
      )
    `);
	}

	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		// ── Internal: Worker tells us to broadcast ──
		if (url.pathname === "/broadcast" && request.method === "POST") {
			const { text } = (await request.json()) as { text: string };
			await this.broadcast(text);
			return new Response("ok");
		}

		// ── WebSocket upgrade ──
		if (request.headers.get("Upgrade") === "websocket") {
			const pair = new WebSocketPair();
			const [client, server] = Object.values(pair);

			this.ctx.acceptWebSocket(server);

			this.sessions.add(server);

			// Replay history so late-joiners see existing cards
			const history = this.getHistory(200);
			server.send(JSON.stringify({ type: "history", items: history }));

			return new Response(null, { status: 101, webSocket: client });
		}

		return new Response("Not found", { status: 404 });
	}

	async webSocketClose(ws: WebSocket): Promise<void> {
		this.sessions.delete(ws);
		ws.close(1000, "closed");
	}

	async webSocketError(ws: WebSocket): Promise<void> {
		this.sessions.delete(ws);
	}

	// ── Helpers ──

	private getHistory(
		limit = 200,
	): { id: number; text: string; createdAt: number }[] {
		const cursor = this.ctx.storage.sql.exec(
			"SELECT id, text, created_at FROM feedback ORDER BY id DESC LIMIT ?",
			limit,
		);

		const items: { id: number; text: string; createdAt: number }[] = [];
		for (const row of cursor) {
			items.push({
				id: row.id as number,
				text: row.text as string,
				createdAt: row.created_at as number,
			});
		}
		// Flip so oldest-first for display
		return items.reverse();
	}

	private insertFeedback(text: string): number {
		const cursor = this.ctx.storage.sql.exec(
			"INSERT INTO feedback (text) VALUES (?) RETURNING id",
			text,
		);
		for (const row of cursor) {
			return row.id as number;
		}
		throw new Error("insert failed");
	}

	private async broadcast(text: string): Promise<void> {
		// Persist
		const id = this.insertFeedback(text);

		// Push to every connected display
		const msg = JSON.stringify({ type: "new", id, text });
		for (const ws of this.sessions) {
			try {
				ws.send(msg);
			} catch {
				this.sessions.delete(ws);
			}
		}
	}
}

// ─────────────────────────────────────────────
// Worker entrypoint — now a class with RPC
// ─────────────────────────────────────────────
export default class extends WorkerEntrypoint {
	// ── RPC method: callable from other Workers via Service Binding ──
	async submitFeedback(text: string): Promise<{ ok: boolean; error?: string }> {
		const trimmed = text?.trim().slice(0, 600);
		if (!trimmed) {
			return { ok: false, error: "empty" };
		}

		const stub = this.env.FEEDBACK_ROOM.getByName("main");

		await stub.fetch("https://internal/broadcast", {
			method: "POST",
			body: JSON.stringify({ text: trimmed }),
		});

		return { ok: true };
	}

	// ── HTTP handler: WebSocket upgrade + optional REST fallback ──
	async fetch(request: Request): Promise<Response> {
		const url = new URL(request.url);

		// ── GET /ws — WebSocket upgrade (proxied to DO) ──
		if (url.pathname === "/ws") {
			const stub = this.env.FEEDBACK_ROOM.getByName("main");
			return stub.fetch(request);
		}

		// ── Everything else falls through to static assets (public/) ──
		// index.html is served automatically at "/"
		return new Response("Not found", { status: 404 });
	}
}

// ─────────────────────────────────────────────
// CORS helpers (adjust origin to your main site)
// ─────────────────────────────────────────────
function corsHeaders(): HeadersInit {
	return {
		"Access-Control-Allow-Origin": "*",
		"Access-Control-Allow-Methods": "POST, OPTIONS",
		"Access-Control-Allow-Headers": "Content-Type",
	};
}
