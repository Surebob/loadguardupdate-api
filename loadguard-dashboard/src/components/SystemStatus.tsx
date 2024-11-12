'use client';

import { useState, useEffect } from 'react';
import { HeartPulse, HeartOff } from 'lucide-react';
import styles from '@/styles/heartbeat.module.css';

interface SystemMetrics {
  cpu_percent: number;
  memory_percent: number;
  timestamp: string;
}

export default function SystemStatus() {
  const [metrics, setMetrics] = useState<SystemMetrics>({
    cpu_percent: 0,
    memory_percent: 0,
    timestamp: ''
  });
  const [isAlive, setIsAlive] = useState(false);

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/status/system');
        const data = await response.json();
        setMetrics(data);
        setIsAlive(true);
      } catch (error) {
        console.error('Error fetching system metrics:', error);
        setIsAlive(false);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 5000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="bg-[#15151B] rounded-lg overflow-hidden h-[280px] flex flex-col">
      <div className="px-6 py-4 border-b border-[#262630]">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-semibold text-white">SYSTEM STATUS</h2>
          {isAlive ? (
            <HeartPulse 
              className={`${styles.heartbeat} text-[#53A762]`} 
              size={20}
            />
          ) : (
            <HeartOff 
              className="text-[#EF4A53]" 
              size={20}
            />
          )}
        </div>
      </div>
      <div className="flex-1 p-6">
        <div className="grid grid-cols-2 gap-4">
          <div className="bg-[#262630] rounded-lg p-4">
            <div className="text-white">CPU</div>
            <div className="text-2xl text-white">{metrics.cpu_percent}%</div>
          </div>
          <div className="bg-[#262630] rounded-lg p-4">
            <div className="text-white">MEMORY</div>
            <div className="text-2xl text-white">{metrics.memory_percent}%</div>
          </div>
        </div>
      </div>
    </div>
  );
} 