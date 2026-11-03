import { KeyboardEvent, PointerEvent, useEffect, useRef, useState } from "react";

export type Point = { x: number; y: number };
export type Series = { name: string; points: Point[] };

type Props = {
  series: Series[];
  marker?: { x: number; label: string };
  dimAfter?: number;
  formatY?: (value: number) => string;
  height?: number;
};

const MARGIN = { top: 20, right: 16, bottom: 28, left: 64 };
const LABELLED = 96;
const LABEL_GAP = 14;
const TICK_GAP = 90;

const day = (x: number) => new Date(x).toISOString().slice(0, 10);
const plain = (value: number) => value.toLocaleString(undefined, { maximumFractionDigits: 2 });

function niceTicks(low: number, high: number, count = 5): number[] {
  if (low === high) return [low];
  const raw = (high - low) / count;
  const power = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * power).find((s) => s >= raw) ?? raw;
  const ticks = [];
  for (let t = Math.ceil(low / step) * step; t <= high + step / 1e6; t += step) ticks.push(t);
  return ticks;
}

function path(points: Point[], sx: (x: number) => number, sy: (y: number) => number) {
  return points.map((p, i) => `${i ? "L" : "M"}${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join("");
}

export function LineChart({ series, marker, dimAfter, formatY = plain, height = 280 }: Props) {
  const frame = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(640);
  const [hover, setHover] = useState<number | null>(null);
  const shown = series.filter((s) => s.points.length > 0).slice(0, 3);
  const xs = [...new Set(shown.flatMap((s) => s.points.map((p) => p.x)))].sort((a, b) => a - b);
  const drawable = xs.length >= 2;

  useEffect(() => {
    const element = frame.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(element);
    return () => observer.disconnect();
  }, [drawable]);

  if (!drawable) return <p className="muted">Not enough points to draw.</p>;

  const right = MARGIN.right + (shown.length > 1 ? LABELLED : 0);
  const ys = shown.flatMap((s) => s.points.map((p) => p.y));
  const [low, high] = [Math.min(...ys), Math.max(...ys)];
  const pad = (high - low) * 0.06 || Math.abs(high) * 0.05 || 1;
  const ticks = niceTicks(low - pad, high + pad);
  const [y0, y1] = [Math.min(ticks[0], low - pad), Math.max(ticks[ticks.length - 1], high + pad)];
  const x0 = Math.min(xs[0], marker?.x ?? Infinity);
  const x1 = Math.max(xs[xs.length - 1], marker?.x ?? -Infinity);
  const innerW = Math.max(width - MARGIN.left - right, 10);
  const innerH = height - MARGIN.top - MARGIN.bottom;
  const sx = (x: number) => MARGIN.left + ((x - x0) / (x1 - x0)) * innerW;
  const sy = (y: number) => MARGIN.top + (1 - (y - y0) / (y1 - y0)) * innerH;
  const stride = Math.max(1, Math.ceil((xs.length - 1) / Math.max(1, Math.floor(innerW / 110))));
  const xTicks = xs.filter((_, i) => i % stride === 0);

  const ends = shown
    .map((s, i) => ({ i, name: s.name, y: sy(s.points[s.points.length - 1].y) }))
    .sort((a, b) => a.y - b.y);
  ends.forEach((e, k) => {
    if (k > 0 && e.y - ends[k - 1].y < LABEL_GAP) e.y = ends[k - 1].y + LABEL_GAP;
  });

  const nearest = (px: number) => {
    let best = 0;
    xs.forEach((x, i) => {
      if (Math.abs(sx(x) - px) < Math.abs(sx(xs[best]) - px)) best = i;
    });
    return best;
  };

  const onMove = (event: PointerEvent<SVGRectElement>) => {
    const box = event.currentTarget.ownerSVGElement!.getBoundingClientRect();
    setHover(nearest(event.clientX - box.left));
  };

  const onKey = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
    event.preventDefault();
    const step = event.key === "ArrowLeft" ? -1 : 1;
    setHover((h) => Math.min(xs.length - 1, Math.max(0, (h ?? xs.length - 1) + step)));
  };

  const hoverX = hover === null ? null : xs[hover];
  const readout =
    hoverX === null
      ? []
      : shown
          .map((s, i) => ({ i, name: s.name, point: s.points.find((p) => p.x === hoverX) }))
          .filter((r) => r.point);
  const tipLeft = hoverX === null ? 0 : Math.min(sx(hoverX) + 12, width - 180);

  return (
    <figure className="chart">
      {shown.length > 1 && (
        <figcaption className="legend">
          {shown.map((s, i) => (
            <span key={s.name}>
              <i className={`key series-${i + 1}`} />
              {s.name}
            </span>
          ))}
        </figcaption>
      )}
      <div
        ref={frame}
        className="chart-frame"
        tabIndex={0}
        onKeyDown={onKey}
        onBlur={() => setHover(null)}
        aria-label={`Line chart of ${shown.map((s) => s.name).join(", ")}; arrow keys read values`}
      >
        <svg width={width} height={height} role="img">
          {ticks.map((t) => (
            <g key={t}>
              <line
                className="grid"
                x1={MARGIN.left}
                x2={MARGIN.left + innerW}
                y1={sy(t)}
                y2={sy(t)}
              />
              <text className="tick" x={MARGIN.left - 8} y={sy(t)} textAnchor="end" dy="0.32em">
                {formatY(t)}
              </text>
            </g>
          ))}
          <line
            className="axis"
            x1={MARGIN.left}
            x2={MARGIN.left + innerW}
            y1={MARGIN.top + innerH}
            y2={MARGIN.top + innerH}
          />
          {xTicks
            .filter((x, i) => i === 0 || sx(x) - sx(xTicks[i - 1]) >= TICK_GAP)
            .map((x) => (
            <text key={x} className="tick" x={sx(x)} y={height - 8} textAnchor="middle">
              {day(x)}
            </text>
          ))}
          {dimAfter !== undefined && dimAfter < x1 && (
            <rect
              className="after"
              x={sx(Math.max(dimAfter, x0))}
              y={MARGIN.top}
              width={MARGIN.left + innerW - sx(Math.max(dimAfter, x0))}
              height={innerH}
            />
          )}
          {shown.map((s, i) => {
            const cut = dimAfter ?? Infinity;
            const before = s.points.filter((p) => p.x <= cut);
            const after = s.points.filter((p) => p.x > cut);
            const bridge = before.length && after.length ? [before[before.length - 1], ...after] : after;
            return (
              <g key={s.name} className={`series-${i + 1}`}>
                {before.length > 1 && <path className="line" d={path(before, sx, sy)} />}
                {bridge.length > 1 && <path className="line dim" d={path(bridge, sx, sy)} />}
              </g>
            );
          })}
          {marker && marker.x >= x0 && marker.x <= x1 && (
            <g className="marker">
              <line
                x1={sx(marker.x)}
                x2={sx(marker.x)}
                y1={MARGIN.top - 6}
                y2={MARGIN.top + innerH}
              />
              <text x={sx(marker.x)} y={MARGIN.top - 8} textAnchor="middle">
                {marker.label}
              </text>
            </g>
          )}
          {shown.length > 1 &&
            ends.map((e) => (
              <text
                key={e.name}
                className="end-label"
                x={MARGIN.left + innerW + 8}
                y={e.y}
                dy="0.32em"
              >
                {e.name}
              </text>
            ))}
          {hoverX !== null && (
            <g>
              <line
                className="crosshair"
                x1={sx(hoverX)}
                x2={sx(hoverX)}
                y1={MARGIN.top}
                y2={MARGIN.top + innerH}
              />
              {readout.map((r) => (
                <circle
                  key={r.name}
                  className={`dot series-${r.i + 1}`}
                  cx={sx(hoverX)}
                  cy={sy(r.point!.y)}
                  r={4}
                />
              ))}
            </g>
          )}
          <rect
            className="hit"
            x={MARGIN.left}
            y={MARGIN.top}
            width={innerW}
            height={innerH}
            onPointerMove={onMove}
            onPointerLeave={() => setHover(null)}
          />
        </svg>
        {hoverX !== null && (
          <div className="tooltip" style={{ left: tipLeft, top: MARGIN.top }}>
            <div className="muted">{day(hoverX)}</div>
            {readout.map((r) => (
              <div key={r.name}>
                <i className={`key series-${r.i + 1}`} />
                <strong>{formatY(r.point!.y)}</strong> <span className="muted">{r.name}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </figure>
  );
}
