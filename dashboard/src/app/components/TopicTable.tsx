'use client';

import React from 'react';

export interface TopicData {
  name: string;
  partitions: Array<{
    id: number;
    messages: number;
    newest_offset: number;
    oldest_offset: number;
  }>;
}

interface TopicTableProps {
  topics: TopicData[];
  onSelectTopic?: (topic: string, partition: number) => void;
}

export function TopicTable({ topics, onSelectTopic }: TopicTableProps) {
  return (
    <div className="animate-fade-in">
      <div className="flex items-center gap-3 mb-4">
        <h2 className="text-lg font-semibold text-zinc-50">Topics</h2>
        <span className="bg-zinc-800 text-zinc-300 text-xs px-2 py-0.5 rounded-full">
          {topics.length}
        </span>
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden">
        {topics.length === 0 ? (
          <div className="p-8 text-center text-zinc-500 text-sm">
            No topics yet
          </div>
        ) : (
          <table className="w-full text-left border-collapse">
            <thead>
              <tr className="border-b border-zinc-800">
                <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Name</th>
                <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Partitions</th>
                <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Total Messages</th>
                <th className="px-4 py-3 text-xs uppercase tracking-wider text-zinc-500 font-medium">Newest Offset</th>
              </tr>
            </thead>
            <tbody>
              {topics.map((topic) => {
                const totalMessages = topic.partitions.reduce((sum, p) => sum + p.messages, 0);
                const newestOffset = topic.partitions.reduce((max, p) => Math.max(max, p.newest_offset), 0);
                return (
                  <tr
                    key={topic.name}
                    onClick={() => onSelectTopic?.(topic.name, topic.partitions[0]?.id ?? 0)}
                    className="border-b border-zinc-800/50 last:border-0 hover:bg-zinc-800/50 transition cursor-pointer"
                  >
                    <td className="px-4 py-3 text-sm text-zinc-300 font-medium">{topic.name}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{topic.partitions.length}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{totalMessages.toLocaleString()}</td>
                    <td className="px-4 py-3 text-sm text-zinc-400">{newestOffset.toLocaleString()}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
