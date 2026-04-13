WITH final AS (
SELECT
  SHA256(CONCAT(ticker, extractTimestamp)) surrogate_key,
  ticker,
  name,
  isin,
  CAST(createdAt AS TIMESTAMP) created_at,
  quantity,
  quantityAvailableForTrading quantity_available_for_trading,
  quantityInPies quantity_in_pies,
  currentPrice current_price,
  averagePricePaid average_price_paid,
  currency,
  totalCost total_cost,
  currentValue current_value,
  unrealizedProfitLoss unrealized_profit,
  fxImpact fx_impact,
  CAST(extractTimestamp AS TIMESTAMP) extract_timestamp
FROM t212.L0_open_positions
)

SELECT * FROM final