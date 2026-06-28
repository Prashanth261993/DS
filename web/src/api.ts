export const API = import.meta.env.VITE_API ?? "http://localhost:8090";
export const WS = import.meta.env.VITE_WS ?? "ws://localhost:8090/ws";
export const GRAFANA = import.meta.env.VITE_GRAFANA ?? "http://localhost:3000";

export interface Bar {
  symbol: string;
  window_start_ms: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  vwap: number;
  count: number;
  sma5: number;
  sma20: number;
}

export async function getSymbols(): Promise<string[]> {
  const r = await fetch(`${API}/symbols`);
  return r.json();
}

export async function getBars(symbol: string, limit = 200): Promise<Bar[]> {
  const r = await fetch(`${API}/bars?symbol=${symbol}&limit=${limit}`);
  const bars: Bar[] = await r.json();
  return bars.sort((a, b) => a.window_start_ms - b.window_start_ms); // oldest first
}
