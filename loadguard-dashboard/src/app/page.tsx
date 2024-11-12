'use client';

import DatasetTable from '@/components/DatasetTable';
import ScheduleManager from '@/components/ScheduleManager';
import ActiveJobs from '@/components/ActiveJobs';
import SystemStatus from '@/components/SystemStatus';

export default function Home() {
  return (
    <div className="min-h-screen bg-[#262630] p-6">
      <div className="space-y-6">
        <div className="flex gap-6 min-w-0">
          <div className="w-3/12 min-w-[300px] shrink-0">
            <SystemStatus />
          </div>
          <div className="w-4/12 min-w-[400px] shrink-0">
            <ScheduleManager />
          </div>
          <div className="w-5/12 min-w-0">
            <ActiveJobs />
          </div>
        </div>
        <DatasetTable hideSystemStatus />
      </div>
    </div>
  );
}
