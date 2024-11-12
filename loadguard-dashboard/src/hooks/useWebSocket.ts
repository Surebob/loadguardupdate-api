import { useEffect, useState } from 'react';

export function useWebSocket<T>(url: string) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    const ws = new WebSocket(url);

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setData(data);
    };

    ws.onerror = (error) => {
      setError(error as Error);
    };

    return () => ws.close();
  }, [url]);

  return { data, error };
} 