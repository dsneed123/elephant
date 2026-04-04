import { NavLink, Outlet } from 'react-router-dom'

const navItems = [
  { to: '/dashboard', label: 'Dashboard' },
  { to: '/traders', label: 'Traders' },
  { to: '/signals', label: 'Signals' },
  { to: '/portfolio', label: 'Portfolio' },
  { to: '/trades', label: 'Trades' },
  { to: '/settings', label: 'Settings' },
]

export default function Layout() {
  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <aside className="w-48 flex-shrink-0 bg-zinc-900 border-r border-zinc-800 flex flex-col">
        <div className="px-5 py-5 border-b border-zinc-800">
          <span className="text-xl font-bold tracking-tight text-white">🐘 Elephant</span>
          <p className="text-xs text-zinc-500 mt-0.5">Kalshi Copy Trader</p>
        </div>
        <nav className="flex-1 px-3 py-4 space-y-1">
          {navItems.map(({ to, label }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `block px-3 py-2 rounded-md text-sm font-medium transition-colors ${
                  isActive
                    ? 'bg-emerald-600 text-white'
                    : 'text-zinc-400 hover:text-white hover:bg-zinc-800'
                }`
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>
        <div className="px-5 py-4 border-t border-zinc-800 text-xs text-zinc-600">
          v0.1.0
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto bg-zinc-950 p-6">
        <Outlet />
      </main>
    </div>
  )
}
