import { useEffect, useState } from 'react';

export function useEventSource<T>(url: string) {
  const [data, setData] = useState<T | null>(null);

  useEffect(() => {
    const eventSource = new EventSource(url);

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setData(data);
    };

    return () => eventSource.close();
  }, [url]);

  return data;
} 