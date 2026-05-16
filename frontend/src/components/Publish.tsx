import { useState } from "react";
import { jpost } from "../api";

export default function Publish() {
  const [author, setAuthor] = useState("");
  const [title, setTitle] = useState("");
  const [body, setBody] = useState("");
  const [kind, setKind] = useState<"poem" | "article">("poem");
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setErr(null);
    try {
      await jpost("/articles", { author, title, body, kind });
      setTitle("");
      setBody("");
    } catch (e: any) {
      setErr(e.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <form onSubmit={submit}>
      <label>Author</label>
      <input value={author} onChange={(e) => setAuthor(e.target.value)} placeholder="Your name" required />
      <label>Title</label>
      <input value={title} onChange={(e) => setTitle(e.target.value)} required />
      <label>Kind</label>
      <select value={kind} onChange={(e) => setKind(e.target.value as any)}>
        <option value="poem">Poem</option>
        <option value="article">Article</option>
      </select>
      <label>Body</label>
      <textarea value={body} onChange={(e) => setBody(e.target.value)} rows={5} required />
      <div style={{ marginTop: 8 }}>
        <button disabled={busy} type="submit">{busy ? "Publishing..." : "Publish"}</button>
      </div>
      {err && <div style={{ color: "var(--pink)", marginTop: 6 }}>{err}</div>}
    </form>
  );
}
