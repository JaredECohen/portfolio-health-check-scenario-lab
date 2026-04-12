import { useDeferredValue, useEffect, useState } from "react";
import { searchTickers } from "../lib/api";
import type { TickerMetadata } from "../types";

interface SearchableTickerInputProps {
  value: string;
  onSelect: (ticker: TickerMetadata) => void;
  placeholder?: string;
}

export function SearchableTickerInput({
  value,
  onSelect,
  placeholder = "Search ticker or company",
}: SearchableTickerInputProps) {
  const [query, setQuery] = useState(value);
  const [results, setResults] = useState<TickerMetadata[]>([]);
  const [open, setOpen] = useState(false);
  const deferredQuery = useDeferredValue(query);

  useEffect(() => {
    setQuery(value);
  }, [value]);

  useEffect(() => {
    let cancelled = false;
    const trimmed = deferredQuery.trim();
    if (trimmed.length < 1) {
      setResults([]);
      return;
    }
    void searchTickers(trimmed)
      .then((items) => {
        if (!cancelled) {
          setResults(items);
        }
      })
      .catch(() => {
        if (!cancelled) {
          setResults([]);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [deferredQuery]);

  return (
    <div className="ticker-search">
      <input
        value={query}
        placeholder={placeholder}
        onChange={(event) => {
          setQuery(event.target.value);
          setOpen(true);
        }}
        onFocus={() => setOpen(true)}
        onBlur={() => {
          window.setTimeout(() => setOpen(false), 150);
        }}
      />
      {open && results.length > 0 ? (
        <div className="ticker-search__menu">
          {results.map((item) => (
            <button
              type="button"
              key={`${item.ticker}-${item.cik}`}
              className="ticker-search__option"
              onClick={() => {
                setQuery(item.ticker);
                setOpen(false);
                onSelect(item);
              }}
            >
              <span>{item.ticker}</span>
              <small>{item.company_name}</small>
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

