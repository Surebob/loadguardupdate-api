import { useEffect, useState } from 'react';
import StatusBadge from './StatusBadge';
import { useEventSource } from '@/hooks/useEventSource';

interface SystemStatus {
  cpu_percent: number;
  memory_percent: number;
  disk_usage: number;
  timestamp: string;
}

type DatasetStatus = 'Needs Update' | 'Updating' | 'Updated' | 'Error';

interface Dataset {
  name: string;
  localDate: string | null;
  serverDate: string;
  status: DatasetStatus;
  progress?: string;
  speed?: string;
}

interface DatasetTableProps {
  hideSystemStatus?: boolean;
}

export default function DatasetTable({ hideSystemStatus = false }: DatasetTableProps) {
  const [datasets, setDatasets] = useState<Record<string, Dataset[]>>({
    socrata: [],
    sms: [],
    ftp: []
  });

  const [systemStatus, setSystemStatus] = useState<SystemStatus>({
    cpu_percent: 0,
    memory_percent: 0,
    disk_usage: 0,
    timestamp: ''
  });

  // Add formatTitle function
  const formatTitle = (type: string): string => {
    switch(type) {
      case 'socrata':
        return 'SOCRATA DATASETS';
      case 'sms':
        return 'SMS DATASETS';
      case 'ftp':
        return 'FTP DATASETS';
      default:
        return type.toUpperCase() + ' DATASETS';
    }
  };

  // Fetch system status
  useEffect(() => {
    const fetchSystemStatus = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/status/system');
        const data = await response.json();
        setSystemStatus(data);
      } catch (error) {
        console.error('Error fetching system status:', error);
      }
    };

    fetchSystemStatus();
    const interval = setInterval(fetchSystemStatus, 5000);
    return () => clearInterval(interval);
  }, []);

  // Get real-time updates from SSE
  const updates = useEventSource<any>('http://localhost:8000/api/updates/stream');

  // Transform status data into dataset format
  const transformStatusData = (data: any): Record<string, Dataset[]> => {
    return {
      socrata: Object.entries(data.socrata || {}).map(([name, info]: [string, any]) => ({
        name,
        localDate: info.local_date,
        serverDate: info.server_date,
        status: info.update_needed ? 'Needs Update' : 'Updated' as DatasetStatus
      })),
      sms: [{
        name: 'SMS File',
        localDate: data.sms?.local_file || null,
        serverDate: data.sms?.server_file || '',
        status: data.sms?.update_needed ? 'Needs Update' : 'Updated' as DatasetStatus
      }],
      ftp: Object.entries(data.ftp || {}).map(([name, info]: [string, any]) => ({
        name,
        localDate: info.local_file,
        serverDate: info.server_file,
        status: info.update_needed ? 'Needs Update' : 'Updated' as DatasetStatus
      }))
    };
  };

  // Handle SSE updates
  useEffect(() => {
    if (updates?.type === 'download') {
      // Update download progress
      const updatedDatasets = { ...datasets };
      Object.entries(updates.datasets).forEach(([name, progress]: [string, any]) => {
        Object.values(updatedDatasets).forEach(datasetList => {
          const dataset = datasetList.find(d => d.name === name);
          if (dataset) {
            dataset.status = 'Updating';
            dataset.progress = progress.progress;
            dataset.speed = progress.speed;
          }
        });
      });
      setDatasets(updatedDatasets);
    }
  }, [updates]);

  // Fetch real data
  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const response = await fetch('http://localhost:8000/api/updates/check');
        const data = await response.json();
        const formattedData = transformStatusData(data);
        setDatasets(formattedData);
      } catch (error) {
        console.error('Error checking updates:', error);
      }
    };

    fetchStatus();
  }, []);

  // Format date only for Socrata datasets
  const formatDateTime = (value: string | null, isSocrata: boolean) => {
    if (!value) return 'Not Found';
    if (!isSocrata) return value; // Return filename as-is for SMS and FTP
    
    const date = new Date(value);
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true
    });
  };

  return (
    <div className="space-y-6">
      {!hideSystemStatus && (
        <div className="bg-[#15151B] rounded-lg overflow-hidden">
          <div className="px-6 py-4">
            <h2 className="text-lg font-semibold text-white">SYSTEM STATUS</h2>
          </div>
          <div className="grid grid-cols-3 gap-4 p-6">
            <div className="bg-[#262630] rounded-lg p-4">
              <div className="text-white">CPU</div>
              <div className="text-2xl text-white">{systemStatus.cpu_percent}%</div>
            </div>
            <div className="bg-[#262630] rounded-lg p-4">
              <div className="text-white">MEMORY</div>
              <div className="text-2xl text-white">{systemStatus.memory_percent}%</div>
            </div>
            <div className="bg-[#262630] rounded-lg p-4">
              <div className="text-white">DISK</div>
              <div className="text-2xl text-white">{systemStatus.disk_usage}%</div>
            </div>
          </div>
        </div>
      )}

      {/* Dataset Tables */}
      {Object.entries(datasets).map(([type, typeDatasets]) => (
        <div key={type} className="bg-[#15151B] rounded-lg overflow-hidden">
          <div className="px-6 py-4 border-b border-[#262630]">
            <h2 className="text-lg font-semibold text-white">{formatTitle(type)}</h2>
          </div>
          <div className="p-6">
            <table className="w-full">
              <colgroup>
                <col className="w-1/4"/>
                <col className="w-[30%]"/>
                <col className="w-[30%]"/>
                <col className="w-[15%]"/>
              </colgroup>
              <thead>
                <tr className="text-[#F5C344] uppercase tracking-wide">
                  <th className="text-left pb-4 font-semibold">Dataset</th>
                  <th className="text-left pb-4 font-semibold">Local Version</th>
                  <th className="text-left pb-4 font-semibold">Server Version</th>
                  <th className="text-left pb-4 font-semibold">Status</th>
                </tr>
              </thead>
              <tbody>
                {typeDatasets.map(dataset => (
                  <tr key={dataset.name} className="border-t border-[#262630]">
                    <td className="py-4 text-white">{dataset.name}</td>
                    <td className="py-4 text-white">
                      {formatDateTime(dataset.localDate, type === 'socrata')}
                    </td>
                    <td className="py-4 text-white">
                      {formatDateTime(dataset.serverDate, type === 'socrata')}
                    </td>
                    <td className="py-4">
                      <StatusBadge status={dataset.status} />
                      {dataset.progress && (
                        <span className="ml-2 text-sm text-white">
                          {dataset.progress} ({dataset.speed})
                        </span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  );
} 