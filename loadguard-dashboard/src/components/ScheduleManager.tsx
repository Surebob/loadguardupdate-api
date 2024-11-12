import { useState } from 'react';

interface TimePickerProps {
  value: string;
  onChange: (time: string) => void;
  label: string;
  align?: 'left' | 'right';
}

const TimePicker = ({ value, onChange, label, align = 'left' }: TimePickerProps) => {
  const hours = Array.from({ length: 24 }, (_, i) => i.toString().padStart(2, '0'));
  const minutes = Array.from({ length: 60 }, (_, i) => i.toString().padStart(2, '0'));

  const [hour, minute] = value.split(':');

  return (
    <div>
      <label className={`text-[#F5C344] font-semibold block uppercase tracking-wide mb-2 text-${align}`}>
        {label}
      </label>
      <div className={`flex gap-2 items-center ${align === 'right' ? 'justify-end' : ''}`}>
        <select 
          value={hour}
          onChange={(e) => onChange(`${e.target.value}:${minute}`)}
          className="bg-[#262630] text-white border border-[#262630] rounded-lg p-2 w-20"
        >
          {hours.map(h => (
            <option key={h} value={h}>
              {h}:00
            </option>
          ))}
        </select>
        <span className="text-white">:</span>
        <select
          value={minute}
          onChange={(e) => onChange(`${hour}:${e.target.value}`)}
          className="bg-[#262630] text-white border border-[#262630] rounded-lg p-2 w-20"
        >
          {minutes.map(m => (
            <option key={m} value={m}>
              {m}
            </option>
          ))}
        </select>
      </div>
    </div>
  );
};

export default function ScheduleManager() {
  const [datasetTime, setDatasetTime] = useState('22:00');
  const [clickerTime, setClickerTime] = useState('23:45');
  const [isUpdating, setIsUpdating] = useState(false);

  const handleSubmit = async () => {
    try {
      setIsUpdating(true);
      const response = await fetch('http://localhost:8000/api/scheduler/update-schedule', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          dataset_time: datasetTime,
          clicker_time: clickerTime,
        }),
      });

      if (!response.ok) throw new Error('Failed to update schedule');
    } catch (error) {
      console.error('Error updating schedule:', error);
    } finally {
      setIsUpdating(false);
    }
  };

  return (
    <div className="bg-[#15151B] rounded-lg overflow-hidden h-[280px] flex flex-col">
      <div className="px-6 py-4 border-b border-[#262630]">
        <h2 className="text-lg font-semibold text-white">SCHEDULE MANAGER</h2>
      </div>
      <div className="flex-1 p-6 flex flex-col justify-between">
        <div className="flex justify-between gap-4 min-w-0 pr-[1px]">
          <div className="min-w-0">
            <TimePicker
              label="Dataset Update Time"
              value={datasetTime}
              onChange={setDatasetTime}
              align="left"
            />
          </div>
          <div className="min-w-0">
            <TimePicker
              label="Clicker Schedule Time"
              value={clickerTime}
              onChange={setClickerTime}
              align="right"
            />
          </div>
        </div>
        <div>
          <button
            onClick={handleSubmit}
            disabled={isUpdating}
            className={`
              px-6 py-2 rounded-lg font-semibold w-full
              ${isUpdating 
                ? 'bg-[#262630] text-[#76777F]' 
                : 'bg-[#F5C344] text-[#15151B] hover:bg-[#F5C344]/90'}
              transition-colors
            `}
          >
            {isUpdating ? 'Updating...' : 'Update Schedule'}
          </button>
        </div>
      </div>
    </div>
  );
} 