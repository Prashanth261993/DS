import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts";
import { API, WS, Bar, getBars, getSymbols } from "./api";

export default function Live() {
  const [symbols, setSymbols] = useState<string[]>([]);
  const [symbol, setSymbol] = useState("BTC-USD");
  const elRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);
  const barsRef = useRef<Bar[]>([]);

  useEffect(() => { getSymbols().then(setSymbols).catch(() => {}); }, []);

  useEffect(() => {
    if (!elRef.current) return;
    const chart = echarts.init(elRef.current, "dark");
    chartRef.current = chart;
    const onResize = () => chart.resize();
    window.addEventListener("resize", onResize);
    return () => { window.removeEventListener("resize", onResize); chart.dispose(); };
  }, []);

  function render() {
    const bars = barsRef.current;
    const cats = bars.map((b) => new Date(b.window_start_ms).toLocaleTimeString());
    chartRef.current?.setOption({
      backgroundColor: "transparent",
      tooltip: { trigger: "axis" },
      legend: { data: ["price", "SMA5", "SMA20"], textStyle: { color: "#ccc" } },
      grid: { left: 60, right: 20, top: 40, bottom: 40 },
      xAxis: { type: "category", data: cats, axisLabel: { color: "#aaa" } },
      yAxis: { scale: true, axisLabel: { color: "#aaa" } },
      series: [
        { name: "price", type: "candlestick", data: bars.map((b) => [b.open, b.close, b.low, b.high]) },
        { name: "SMA5", type: "line", smooth: true, showSymbol: false, data: bars.map((b) => b.sma5) },
        { name: "SMA20", type: "line", smooth: true, showSymbol: false, data: bars.map((b) => b.sma20) },
      ],
    });
  }

  useEffect(() => {
    let ws: WebSocket | null = null;
    getBars(symbol, 200).then((b) => { barsRef.current = b; render(); });
    ws = new WebSocket(WS);
    ws.onmessage = (e) => {
      const bar: Bar = JSON.parse(e.data);
      if (bar.symbol !== symbol) return;
      const arr = barsRef.current;
      const last = arr[arr.length - 1];
      if (last && last.window_start_ms === bar.window_start_ms) arr[arr.length - 1] = bar;
      else arr.push(bar);
      if (arr.length > 200) arr.shift();
      render();
    };
    return () => ws?.close();
  }, [symbol]);

  return (
    <div>
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 8 }}>
        <strong>Symbol</strong>
        <select value={symbol} onChange={(e) => setSymbol(e.target.value)}>
          {symbols.map((s) => <option key={s}>{s}</option>)}
        </select>
        <span style={{ color: "#888" }}>live via {API}/ws</span>
      </div>
      <div ref={elRef} style={{ width: "100%", height: 520 }} />
    </div>
  );
}
