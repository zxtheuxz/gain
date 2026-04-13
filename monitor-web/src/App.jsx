import { useEffect, useMemo, useState } from "react";

const DATA_URL = `${import.meta.env.BASE_URL}data/asset-monitor.json`;

function formatPct(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${Number(value).toFixed(2)}%`;
}

function formatPf(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  if (Number(value) >= 9999) {
    return "INF";
  }
  return Number(value).toFixed(2);
}

function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0";
  }
  return new Intl.NumberFormat("pt-BR").format(Number(value));
}

function formatDate(value) {
  if (!value) {
    return "n/a";
  }
  const [year, month, day] = value.split("-");
  return `${day}/${month}/${year}`;
}

function KpiCard({ eyebrow, value, note, accent = "gold" }) {
  return (
    <article className={`kpi-card kpi-card--${accent}`}>
      <span className="kpi-eyebrow">{eyebrow}</span>
      <strong className="kpi-value">{value}</strong>
      <span className="kpi-note">{note}</span>
    </article>
  );
}

function StrategyBadge({ signal }) {
  return (
    <div className={`signal-badge signal-badge--${signal.trade_direction}`}>
      <span>{signal.action_label}</span>
      <strong>{signal.entry_rule.toUpperCase()}</strong>
    </div>
  );
}

function getConfidenceLabel(signal) {
  if (signal.is_qualified_ticker) {
    return "Prioridade";
  }
  if (signal.strategy_metrics.profit_factor >= 2.5 && signal.strategy_metrics.average_trade_return_pct >= 1.3) {
    return "Forte";
  }
  return "Observacao";
}

function getWinRate(metrics) {
  return metrics?.profitable_trade_rate_pct ?? metrics?.success_rate_pct;
}

function getSignalScore(signal) {
  const tickerMetrics = signal.ticker_metrics ?? {};
  return (
    Number(tickerMetrics.average_trade_return_pct ?? 0) * 100 +
    Number(getWinRate(tickerMetrics) ?? 0) * 2 +
    Math.min(Number(tickerMetrics.profit_factor ?? 0), 20) * 5 +
    Math.min(Number(tickerMetrics.total_trades ?? 0), 120) * 0.3 +
    Number(signal.strategy_metrics.average_trade_return_pct ?? 0) * 30 +
    Number(getWinRate(signal.strategy_metrics) ?? 0)
  );
}

function App() {
  const [payload, setPayload] = useState(null);
  const [status, setStatus] = useState("loading");
  const [error, setError] = useState("");
  const [search, setSearch] = useState("");
  const [showQualifiedOnly, setShowQualifiedOnly] = useState(true);
  const [entryFilter, setEntryFilter] = useState("all");

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setStatus("loading");
        setError("");
        const response = await fetch(DATA_URL, { cache: "no-store" });
        if (!response.ok) {
          throw new Error("Nao foi possivel carregar o JSON do monitor.");
        }
        const data = await response.json();
        if (!cancelled) {
          setPayload(data);
          setStatus("ready");
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Falha ao carregar o monitor.");
          setStatus("error");
        }
      }
    }

    load();
    const intervalId = window.setInterval(load, 60_000);
    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  const filteredSignals = useMemo(() => {
    if (!payload) {
      return [];
    }
    return payload.signals.filter((signal) => {
      if (showQualifiedOnly && !signal.is_qualified_ticker) {
        return false;
      }
      if (entryFilter !== "all" && signal.entry_rule !== entryFilter) {
        return false;
      }
      if (!search) {
        return true;
      }
      const haystack = [
        signal.ticker,
        signal.strategy_label,
        signal.state_signature,
        signal.action_label,
      ]
        .join(" ")
        .toLowerCase();
      return haystack.includes(search.toLowerCase());
    });
  }, [entryFilter, payload, search, showQualifiedOnly]);

  const topStrategies = useMemo(() => {
    if (!payload) {
      return [];
    }
    return payload.strategies.slice(0, 12);
  }, [payload]);

  const prioritySignals = useMemo(() => {
    if (!payload) {
      return [];
    }
    const grouped = new Map();
    for (const signal of payload.signals.filter((item) => item.is_qualified_ticker)) {
      const existing = grouped.get(signal.ticker);
      const score = getSignalScore(signal);
      if (!existing || score > existing.score) {
        grouped.set(signal.ticker, { best: signal, count: (existing?.count ?? 0) + 1, score });
      } else {
        existing.count += 1;
      }
    }
    return [...grouped.values()]
      .map((item) => ({
        ...item.best,
        confirming_signals_count: item.count,
        operational_score: item.score,
      }))
      .sort((left, right) => right.operational_score - left.operational_score)
      .slice(0, 12);
  }, [payload]);

  if (status === "loading" && !payload) {
    return (
      <main className="app-shell">
        <section className="hero">
          <p className="hero-eyebrow">Monitor Quantitativo B3</p>
          <h1>Carregando sinais e estratégias monitoradas.</h1>
        </section>
      </main>
    );
  }

  if (status === "error" && !payload) {
    return (
      <main className="app-shell">
        <section className="hero">
          <p className="hero-eyebrow">Monitor Quantitativo B3</p>
          <h1>O app nao conseguiu carregar o JSON do monitor.</h1>
          <p className="hero-copy">{error}</p>
          <div className="callout">
            <p>Atualize o payload com:</p>
            <code>python -m b3_patterns asset-monitor-export</code>
          </div>
        </section>
      </main>
    );
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-copy-wrap">
          <p className="hero-eyebrow">Monitor Quantitativo B3</p>
          <h1>Identifique rapidamente quando as estratégias finais forem acionadas.</h1>
          <p className="hero-copy">
            Este painel acompanha as melhores estratégias elite com lucro médio mínimo de 1% por trade.
            O app lê o JSON exportado do projeto Python e mostra, por papel, quando o estado estatístico do mercado
            voltou a aparecer.
          </p>
        </div>
        <div className="hero-panel">
          <span className="hero-panel-label">Último pregão na base</span>
          <strong>{formatDate(payload.latest_trade_date)}</strong>
          <span className="hero-panel-label">Payload gerado em</span>
          <strong>{formatDate(payload.generated_at)}</strong>
          <p className="hero-panel-note">
            Base diária local. O monitor identifica sinais no último pregão disponível do banco.
          </p>
        </div>
      </section>

      <section className="kpi-grid">
        <KpiCard
          eyebrow="Estratégias monitoradas"
          value={formatNumber(payload.strategies_monitored)}
          note={`${formatNumber(payload.strategies_available ?? payload.strategies_monitored)} disponíveis na shortlist`}
        />
        <KpiCard
          eyebrow="Sinais encontrados"
          value={formatNumber(payload.signals_triggered)}
          note="Ocorrências atuais em todo o universo"
          accent="sage"
        />
        <KpiCard
          eyebrow="Sinais em ações qualificadas"
          value={formatNumber(payload.signals_triggered_qualified)}
          note="Papéis que também passaram no corte por ação"
          accent="ember"
        />
        <KpiCard
          eyebrow="Tickers acionados"
          value={formatNumber(payload.triggered_tickers)}
          note={`Universo observado: ${formatNumber(payload.universe_size)} ações`}
          accent="slate"
        />
      </section>

      <section className="guides-grid">
        <article className="guide-card">
          <h2>Como ler em 30 segundos</h2>
          <ol>
            <li>Comece pela seção `Ativou agora`.</li>
            <li>Se o card estiver como `Prioridade`, a ação também passou no backtest individual.</li>
            <li>`Entrada` mostra se a estratégia é para abertura ou fechamento.</li>
            <li>`Alvo` e `Stop` são o plano percentual validado no backtest.</li>
          </ol>
        </article>
        <article className="guide-card">
          <h2>O que importa</h2>
          <ul>
            <li>`Média`: lucro médio por trade no backtest.</li>
            <li>`PF`: quanto o padrão ganhou para cada 1 perdido.</li>
            <li>`Acerto`: percentual de operações vencedoras.</li>
            <li>`Ticker qualificado`: ação com histórico próprio dentro da estratégia.</li>
          </ul>
        </article>
      </section>

      <section className="section-header">
        <div>
          <p className="section-eyebrow">Ativou agora</p>
          <h2>Ações para olhar primeiro</h2>
        </div>
      </section>

      <section className="activated-grid">
        {prioritySignals.length === 0 ? (
          <article className="empty-state">
            <h3>Nenhuma ação prioritária acionada.</h3>
            <p>Existem sinais no radar, mas nenhum passou pelo corte individual do ticker neste pregão.</p>
          </article>
        ) : (
          prioritySignals.map((signal) => (
            <article key={`priority-${signal.signal_id}`} className="activated-card">
              <div className="activated-top">
                <div>
                  <span className="activated-label">{getConfidenceLabel(signal)}</span>
                  <h3>{signal.ticker}</h3>
                </div>
                <strong>{signal.action_label}</strong>
              </div>

              <div className="plan-strip">
                <div>
                  <span>Entrada</span>
                  <strong>{signal.entry_rule === "open" ? "Abertura" : "Fechamento"}</strong>
                </div>
                <div>
                  <span>Alvo</span>
                  <strong>{formatPct(signal.take_profit_pct)}</strong>
                </div>
                <div>
                  <span>Stop</span>
                  <strong>-{formatPct(signal.stop_loss_pct)}</strong>
                </div>
                <div>
                  <span>Prazo limite</span>
                  <strong>{signal.time_cap_days}D</strong>
                </div>
              </div>

              <p className="plain-trigger">
                <strong>Por que ativou:</strong> {signal.state_signature}.
              </p>
              {signal.confirming_signals_count > 1 ? (
                <p className="plain-trigger">
                  <strong>Confirmação:</strong> {signal.confirming_signals_count} estratégias operacionais acionaram este papel; o card mostra a melhor pelo score.
                </p>
              ) : null}

              <div className="activated-metrics">
                <div>
                  <span>Backtest da estratégia</span>
                  <strong>{formatPct(signal.strategy_metrics.average_trade_return_pct)}</strong>
                  <small>média | PF {formatPf(signal.strategy_metrics.profit_factor)}</small>
                </div>
                <div>
                  <span>Backtest da ação</span>
                  <strong>{formatPct(signal.ticker_metrics.average_trade_return_pct)}</strong>
                  <small>{formatNumber(signal.ticker_metrics.total_trades)} trades | PF {formatPf(signal.ticker_metrics.profit_factor)}</small>
                </div>
              </div>
            </article>
          ))
        )}
      </section>

      <section className="section-header">
        <div>
          <p className="section-eyebrow">Todos os sinais</p>
          <h2>Radar detalhado</h2>
        </div>
        <div className="toolbar">
          <input
            className="search-input"
            type="search"
            placeholder="Filtrar por ticker ou estratégia"
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <select value={entryFilter} onChange={(event) => setEntryFilter(event.target.value)}>
            <option value="all">Todas as entradas</option>
            <option value="open">Só open</option>
            <option value="close">Só close</option>
          </select>
          <label className="toggle">
            <input
              type="checkbox"
              checked={showQualifiedOnly}
              onChange={(event) => setShowQualifiedOnly(event.target.checked)}
            />
            <span>Somente ações qualificadas</span>
          </label>
        </div>
      </section>

      <section className="signals-grid">
        {filteredSignals.length === 0 ? (
          <article className="empty-state">
            <h3>Nenhum sinal encontrado com o filtro atual.</h3>
            <p>Se quiser ampliar o radar, desmarque o filtro de ações qualificadas.</p>
          </article>
        ) : (
          filteredSignals.map((signal) => (
            <article key={signal.signal_id} className={`signal-card ${signal.is_qualified_ticker ? "signal-card--qualified" : ""}`}>
              <div className="signal-card-top">
                <div>
                  <p className="signal-ticker">{signal.ticker}</p>
                  <p className="signal-date">Pregão: {formatDate(signal.trade_date)}</p>
                </div>
                <StrategyBadge signal={signal} />
              </div>

              <h3>{signal.strategy_label}</h3>
              <p className="signal-state">{signal.state_signature}</p>
              <p className="signal-how">{signal.how_to_operate}</p>

              <div className="metrics-row">
                <div>
                  <span>Estratégia</span>
                  <strong>{formatPct(signal.strategy_metrics.average_trade_return_pct)}</strong>
                  <small>média por trade</small>
                </div>
                <div>
                  <span>Acerto</span>
                  <strong>{formatPct(getWinRate(signal.strategy_metrics))}</strong>
                  <small>{formatNumber(signal.strategy_metrics.trades)} trades</small>
                </div>
                <div>
                  <span>PF</span>
                  <strong>{formatPf(signal.strategy_metrics.profit_factor)}</strong>
                  <small>net {formatPct(signal.strategy_metrics.net_trade_return_pct)}</small>
                </div>
              </div>

              <div className="signal-price-box">
                <span>OHLC do último pregão</span>
                <strong>
                  O {signal.price_snapshot.open.toFixed(2)} | H {signal.price_snapshot.high.toFixed(2)} | L {signal.price_snapshot.low.toFixed(2)} | C {signal.price_snapshot.close.toFixed(2)}
                </strong>
              </div>

              {signal.ticker_metrics ? (
                <div className="ticker-chip ticker-chip--good">
                  <strong>Ticker qualificado</strong>
                  <span>
                    {formatNumber(signal.ticker_metrics.total_trades)} trades | média {formatPct(signal.ticker_metrics.average_trade_return_pct)} | PF{" "}
                    {formatPf(signal.ticker_metrics.profit_factor)}
                  </span>
                </div>
              ) : (
                <div className="ticker-chip">
                  <strong>Ticker sem qualificação individual</strong>
                  <span>O sinal existe, mas o papel não passou no corte mínimo por ação.</span>
                </div>
              )}

              <div className="states-list">
                {signal.matched_states.map((state) => (
                  <div key={`${signal.signal_id}-${state.feature_key}`} className="state-pill">
                    <span>{state.feature_label}</span>
                    <strong>{state.bucket_label}</strong>
                  </div>
                ))}
              </div>
            </article>
          ))
        )}
      </section>

      <section className="section-header">
        <div>
          <p className="section-eyebrow">Shortlist operacional</p>
          <h2>Estratégias monitoradas</h2>
        </div>
      </section>

      <section className="strategy-table-wrap">
        <table className="strategy-table">
          <thead>
            <tr>
              <th>Estratégia</th>
              <th>Acerto</th>
              <th>Média</th>
              <th>PF</th>
              <th>Triggers</th>
              <th>Triggers qualificados</th>
            </tr>
          </thead>
          <tbody>
            {topStrategies.map((strategy) => (
              <tr key={strategy.code}>
                <td>
                  <strong>{strategy.label}</strong>
                  <span>{strategy.state_signature}</span>
                </td>
                <td>{formatPct(getWinRate(strategy.metrics))}</td>
                <td>{formatPct(strategy.metrics.average_trade_return_pct)}</td>
                <td>{formatPf(strategy.metrics.profit_factor)}</td>
                <td>{formatNumber(strategy.triggered_count)}</td>
                <td>{formatNumber(strategy.qualified_triggered_count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="section-header">
        <div>
          <p className="section-eyebrow">Papéis mais recorrentes</p>
          <h2>Onde olhar primeiro</h2>
        </div>
      </section>

      <section className="ticker-grid">
        {payload.top_triggered_tickers.slice(0, 12).map((item) => (
          <article key={item.ticker} className="ticker-card">
            <div>
              <p className="ticker-card-symbol">{item.ticker}</p>
              <p className="ticker-card-count">{formatNumber(item.signals)} sinais atuais</p>
            </div>
            {item.overall ? (
              <div className="ticker-card-meta">
                <span>{item.overall.elite_strategies_count} estratégias elite</span>
                <strong>média {formatPct(item.overall.average_of_average_trade_return_pct)}</strong>
              </div>
            ) : (
              <div className="ticker-card-meta">
                <span>Sem histórico consolidado qualificado</span>
              </div>
            )}
          </article>
        ))}
      </section>
    </main>
  );
}

export default App;
