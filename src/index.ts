import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";

// ─────────────────────────────────────────────
// Durable Object: manages WebSocket connections
// and broadcasts new feedback to all displays.
// ─────────────────────────────────────────────
export class FeedbackRoom extends DurableObject {
	private sessions: Set<WebSocket> = new Set();

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);

		// Ensure the history table exists (runs once per DO instance)
		this.ctx.storage.sql.exec(`
			CREATE TABLE IF NOT EXISTS history (
				id INTEGER PRIMARY KEY,
				data TEXT NOT NULL
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
			const history = await this.getHistory();
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

	private async getHistory(): Promise<string[]> {
		const cursor = this.ctx.storage.sql.exec(
			"SELECT data FROM history WHERE id = 1",
		);
		for (const row of cursor) {
			return JSON.parse(row.data as string);
		}
		return [];
	}

	private async setHistory(items: string[]): Promise<void> {
		await this.ctx.storage.sql.exec(
			"INSERT OR REPLACE INTO history (id, data) VALUES (1, ?)",
			JSON.stringify(items),
		);
	}

	private async broadcast(text: string): Promise<void> {
		// Persist
		const history = await this.getHistory();
		history.push(text);
		await this.setHistory(history);

		// Push to every connected display
		const msg = JSON.stringify({ type: "new", text });
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
