'use client';

import React from 'react';

interface StatCardProps {
  icon: React.ComponentType<{ size?: number; className?: string }>;
  label: string;
  value: string | number;
}

export function StatCard({ icon: Icon, label, value }: StatCardProps) {
  return (
    <div className="bg-zinc-900 border border-zinc-800 rounded-xl p-5 animate-fade-in flex flex-col">
      <div className="text-zinc-500">
        <Icon size={20} />
      </div>
      <div className="text-sm font-medium text-zinc-400 mt-3">{label}</div>
      <div className="text-zinc-50 text-2xl font-semibold tracking-tight mt-1">
        {typeof value === 'number' ? value.toLocaleString() : value}
      </div>
    </div>
  );
}
