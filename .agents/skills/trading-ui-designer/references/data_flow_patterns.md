# Trading Data Flow Patterns

## 1. Dynamic WebSocket Routing
*   **Backend Hub**: Maintain a single connection to the exchange and multiplex data to specific UI channels.
*   **On-demand Subscription**: Subscribe/Unsubscribe from market data streams dynamically based on UI active symbols.

## 2. Real-time Synchronization
*   **Chart Data**: Backend computes OHLCV and indicators, then pushes updates to UI.
*   **Order Book**: Aggregate high-frequency updates (e.g., 100ms) before sending to UI to prevent browser freezing.
*   **Account Data**: Use event-driven pushes for balance changes and order executions.

## 3. Concurrency & Safety
*   **Threading**: Use dedicated threads for data collection, strategy execution, and UI communication.
*   **Locks**: Ensure thread-safe access to shared account and order data.
