'use client';

import React from 'react';

export interface ConsumerGroupData {
  group_id: string;
  offsets: Record<string, Record<string, {
    committed: number;
    latest: number;
    lag: number;
  }>>;
}

interface ConsumerGroupsProps {
  groups: ConsumerGroupData[];
}

export function ConsumerGroups({ groups }: ConsumerGroupsProps) {
  const rows = groups.flatMap((group) => 
    Object.entries(group.offsets).flatMap(([topic, partitions]) => 
      Object.entries(partitions).map(([partition, data]) => ({
        group_id: group.group_id,
        topic,
        partition,
        ...data
      }))
    )
  );

  return (
    <div className="animate-fade-in flex flex-col h-full">
      <div className="flex items-center gap-3 mb-4">
        <h2 className="text-lg font-semibold text-zinc-50">Consumer Groups</h2>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden flex-1">
        {rows.length === 0 ? (
          <div className="h-full flex items-center justify-center text-zinc-500 text-sm p-8">
            No consumer groups
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="border-b border-zinc-800">
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Group</th>
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Topic</th>
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Partition</th>
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Committed</th>
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Latest</th>
                  <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Lag</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => (
                  <tr key={i} className="border-b border-zinc-800/50 last:border-0 hover:bg-zinc-800/50 transition">
                    <td className="px-4 py-3 text-sm text-zinc-300">{row.group_id}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{row.topic}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{row.partition}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400 font-mono">{row.committed.toLocaleString()}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400 font-mono">{row.latest.toLocaleString()}</td>
                    <td className={`px-4 py-3 text-sm font-mono font-medium ${row.lag > 0 ? 'text-zinc-300' : 'text-zinc-500'}`}>
                      {row.lag.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
