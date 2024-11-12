export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="min-h-screen bg-[var(--secondary-color)]">
      <nav className="bg-[var(--accent2-color)] border-b border-[var(--details2-color)] shadow-md">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <h1 className="text-xl font-bold text-[var(--text-color)]">LoadGuard Dashboard</h1>
        </div>
      </nav>
      <main className="container mx-auto p-6">
        {children}
      </main>
    </div>
  );
} 