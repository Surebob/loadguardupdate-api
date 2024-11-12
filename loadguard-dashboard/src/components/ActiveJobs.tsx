'use client';

import { useState, useEffect } from 'react';
import { useEventSource } from '@/hooks/useEventSource';

interface Job {
  id: string;
  name: string;
  next_run: string;
  running: boolean;
}

interface SchedulerStatus {
  scheduler_running: boolean;
  jobs: Record<string, Job>;
  current_time: string;
}

interface UpdateProgress {
  type: string;
  status: string;
  details: {
    [key: string]: {
      status: string;
      progress: string;
      speed: string;
    }
  }
}

export default function ActiveJobs() {
  const [status, setStatus] = useState<SchedulerStatus | null>(null);
  const [loadingJobs, setLoadingJobs] = useState<Record<string, boolean>>({});
  const updates = useEventSource<UpdateProgress>('http://localhost:8000/api/updates/stream');

  // Handle real-time updates
  useEffect(() => {
    if (updates?.type === 'in_progress' && updates.details) {
      setLoadingJobs(prev => {
        const newState = { ...prev };
        Object.keys(updates.details).forEach(jobId => {
          newState[jobId] = true;
        });
        return newState;
      });
    } else if (updates?.type === 'completed') {
      setLoadingJobs({});
    }
  }, [updates]);

  const fetchStatus = async () => {
    try {
      const response = await fetch('http://localhost:8000/api/scheduler/status');
      const data = await response.json();
      setStatus(data);
    } catch (error) {
      console.error('Error fetching scheduler status:', error);
    }
  };

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 1000);
    return () => clearInterval(interval);
  }, []);

  const handlePauseResume = async () => {
    if (!status) return;
    try {
      const endpoint = status.scheduler_running ? 'pause' : 'resume';
      await fetch(`http://localhost:8000/api/scheduler/${endpoint}`, {
        method: 'POST'
      });
      await fetchStatus();
    } catch (error) {
      console.error('Error updating scheduler:', error);
    }
  };

  const handleTrigger = async (jobId: string) => {
    try {
      setLoadingJobs(prev => ({ ...prev, [jobId]: true }));
      if (jobId === 'dataset_update') {
        await fetch('http://localhost:8000/api/updates/trigger', {
          method: 'POST'
        });
      } else if (jobId === 'clicker_job') {
        await fetch('http://localhost:8000/api/scheduler/trigger-clicker', {
          method: 'POST'
        });
      }
      await fetchStatus();
    } catch (error) {
      console.error('Error triggering job:', error);
    } finally {
      setLoadingJobs(prev => ({ ...prev, [jobId]: false }));
    }
  };

  const formatNextRun = (dateString: string) => {
    const date = new Date(dateString);
    return date.toLocaleString('en-US', {
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true
    });
  };

  if (!status) return null;

  return (
    <div className="bg-[#15151B] rounded-lg overflow-hidden h-[280px] flex flex-col">
      <div className="px-6 border-b border-[#262630]">
        <div className="flex justify-between items-center">
          <h2 className="text-lg py-4 font-semibold text-white">ACTIVE JOBS</h2>
          <button
            onClick={handlePauseResume}
            className={`
              px-4 py-3 rounded-lg font-semibold text-sm
              ${status?.scheduler_running 
                ? 'bg-[#EF4A53]/10 text-[#EF4A53] border border-[#EF4A53]/20' 
                : 'bg-[#53A762]/10 text-[#53A762] border border-[#53A762]/20'}
              hover:bg-opacity-20 transition-colors
            `}
          >
            {status?.scheduler_running ? 'Pause Scheduler' : 'Resume Scheduler'}
          </button>
        </div>
      </div>
      <div className="flex-1 p-6 flex flex-col justify-between">
        <div className="space-y-3">
          {Object.entries(status?.jobs || {}).map(([id, job]) => (
            <div key={id} className="bg-[#262630] rounded-lg p-4 flex items-center justify-between">
              <div>
                <h3 className="text-[#F5C344] font-semibold mb-1">
                  {id === 'dataset_update' ? 'Dataset Updates' : 'Mouse Clicker'}
                </h3>
                <p className="text-white text-sm">
                  Next run: {formatNextRun(job.next_run)}
                </p>
              </div>
              <button
                onClick={() => handleTrigger(id)}
                disabled={loadingJobs[id] || !status?.scheduler_running}
                className={`
                  px-4 py-2 rounded-lg font-semibold text-sm
                  ${loadingJobs[id] || !status?.scheduler_running
                    ? 'bg-[#262630] text-[#76777F]'
                    : 'bg-[#F5C344] text-[#15151B] hover:bg-[#F5C344]/90'}
                  transition-colors
                `}
              >
                Run Now
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
} 