import { describe, expect, it } from "vitest";
import {
  applyQuoteToHolding,
  clearCalculatedHoldingValue,
  updateHoldingFromMarketValue,
  updateHoldingFromShares,
} from "./holdingSync";
import type { HoldingRow, TickerQuote } from "../types";

function buildHolding(overrides: Partial<HoldingRow> = {}): HoldingRow {
  return {
    ticker: "AAPL",
    shares: "",
    market_value: "",
    company_name: "Apple Inc",
    sector: "Technology",
    latest_price: 200,
    price_as_of: "2025-01-10",
    last_edited: null,
    ...overrides,
  };
}

const quote: TickerQuote = {
  ticker: "AAPL",
  price: 200,
  as_of: "2025-01-10",
};

describe("holdingSync", () => {
  it("recomputes market value when shares change", () => {
    expect(updateHoldingFromShares(buildHolding(), "2.5")).toMatchObject({
      shares: "2.5",
      market_value: "500",
      last_edited: "shares",
    });
  });

  it("recomputes shares when market value changes", () => {
    expect(updateHoldingFromMarketValue(buildHolding(), "750")).toMatchObject({
      market_value: "750",
      shares: "3.75",
      last_edited: "market_value",
    });
  });

  it("hydrates the missing market value when a quote arrives", () => {
    expect(
      applyQuoteToHolding(
        buildHolding({
          latest_price: null,
          price_as_of: null,
          shares: "4",
          market_value: "",
          last_edited: "shares",
        }),
        quote,
      ),
    ).toMatchObject({
      shares: "4",
      market_value: "800",
      latest_price: 200,
      price_as_of: "2025-01-10",
    });
  });

  it("keeps the manual market value and clears shares when the quote is reset", () => {
    expect(
      clearCalculatedHoldingValue(
        buildHolding({
          shares: "3.75",
          market_value: "750",
          last_edited: "market_value",
        }),
      ),
    ).toMatchObject({
      shares: "",
      market_value: "750",
      latest_price: null,
      price_as_of: null,
    });
  });
});
