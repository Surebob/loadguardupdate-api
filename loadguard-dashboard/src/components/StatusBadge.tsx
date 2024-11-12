interface StatusBadgeProps {
  status: 'Needs Update' | 'Updating' | 'Updated' | 'Error';
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const getStatusStyles = () => {
    switch (status) {
      case 'Needs Update':
        return 'bg-[#F5C344]/10 text-[#F5C344] border-[#F5C344]/20';
      case 'Updating':
        return 'bg-[#76777F]/10 text-[#76777F] border-[#76777F]/20';
      case 'Updated':
        return 'bg-[#53A762]/10 text-[#53A762] border-[#53A762]/20';
      case 'Error':
        return 'bg-[#EF4A53]/10 text-[#EF4A53] border-[#EF4A53]/20';
      default:
        return '';
    }
  };

  return (
    <span className={`px-3 py-1 rounded-full border ${getStatusStyles()}`}>
      {status}
    </span>
  );
} 