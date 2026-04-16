import type { HoldingRow, TickerQuote } from "../types";

function trimTrailingZeros(value: string): string {
  return value.replace(/(\.\d*?[1-9])0+$/, "$1").replace(/\.0+$/, "").replace(/\.$/, "");
}

export function parsePositiveNumber(value: string): number | null {
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return null;
  }
  return parsed;
}

export function formatShares(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "";
  }
  return trimTrailingZeros(value.toFixed(4));
}

export function formatMarketValue(value: number): string {
  if (!Number.isFinite(value) || value <= 0) {
    return "";
  }
  return trimTrailingZeros(value.toFixed(2));
}

export function updateHoldingFromShares(holding: HoldingRow, sharesInput: string): HoldingRow {
  const shares = parsePositiveNumber(sharesInput);
  return {
    ...holding,
    shares: sharesInput,
    market_value:
      shares !== null && holding.latest_price ? formatMarketValue(shares * holding.latest_price) : "",
    last_edited: "shares",
  };
}

export function updateHoldingFromMarketValue(
  holding: HoldingRow,
  marketValueInput: string,
): HoldingRow {
  const marketValue = parsePositiveNumber(marketValueInput);
  return {
    ...holding,
    market_value: marketValueInput,
    shares:
      marketValue !== null && holding.latest_price
        ? formatShares(marketValue / holding.latest_price)
        : "",
    last_edited: "market_value",
  };
}

export function clearCalculatedHoldingValue(holding: HoldingRow): HoldingRow {
  const hasMarketValue = parsePositiveNumber(holding.market_value) !== null;
  if (holding.last_edited === "market_value" || (!holding.shares && hasMarketValue)) {
    return {
      ...holding,
      shares: "",
      latest_price: null,
      price_as_of: null,
    };
  }
  return {
    ...holding,
    market_value: "",
    latest_price: null,
    price_as_of: null,
  };
}

export function applyQuoteToHolding(holding: HoldingRow, quote: TickerQuote): HoldingRow {
  const withQuote: HoldingRow = {
    ...holding,
    latest_price: quote.price,
    price_as_of: quote.as_of,
  };
  if (withQuote.last_edited === "market_value") {
    return updateHoldingFromMarketValue(withQuote, withQuote.market_value);
  }
  if (withQuote.shares) {
    return updateHoldingFromShares(withQuote, withQuote.shares);
  }
  if (withQuote.market_value) {
    return updateHoldingFromMarketValue(withQuote, withQuote.market_value);
  }
  return withQuote;
}

export function hasValidShareCount(holding: HoldingRow): boolean {
  return parsePositiveNumber(holding.shares) !== null;
}
