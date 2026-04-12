WITH final AS (
select
  "id" as id,
  "ticker" as ticker,
  "name" as name,
  "isin" as isin,
  "createdAt"::timestamptz as created_at,
  "quantity" as quantity,
  "quantityAvailableForTrading" as quantity_available_for_trading,
  "quantityInPies" as quantity_in_pies,
  "currentPrice" as current_price,
  "averagePricePaid" as average_price_paid,
  "currency" as currency,
  "totalCost" as total_cost,
  "currentValue" as current_value,
  "unrealizedProfitLoss" as unrealized_profit_loss,
  "fxImpact" as fx_impact,
  ("extract_timestamp" at time zone 'UTC') as extract_timestamp
from t212."L0_active_positions"
)

SELECT * FROM final