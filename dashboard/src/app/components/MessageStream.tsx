'use client';

import React, { useEffect, useRef } from 'react';

export interface MessageData {
  offset: number;
  timestamp: number;
  key: string;
  value: string;
  topic?: string;
  partition?: number;
}

interface MessageStreamProps {
  messages: MessageData[];
  selectedTopic: string;
  selectedPartition: number;
}

export function MessageStream({ messages, selectedTopic, selectedPartition }: MessageStreamProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (containerRef.current) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [messages]);

  return (
    <div className="flex flex-col h-full animate-fade-in">
      <div className="flex items-center gap-3 mb-4">
        <h2 className="text-lg font-semibold text-zinc-50 flex items-center gap-2">
          Live Messages
          <span className="w-2 h-2 rounded-full bg-zinc-400 animate-pulse-subtle" />
        </h2>
        {selectedTopic && (
          <span className="bg-zinc-800 text-zinc-300 text-xs px-2 py-0.5 rounded-full font-mono">
            {selectedTopic}:{selectedPartition}
          </span>
        )}
      </div>

      <div className="bg-zinc-900 border border-zinc-800 rounded-xl overflow-hidden flex-1 flex flex-col min-h-[400px]">
        <div ref={containerRef} className="flex-1 overflow-y-auto p-2">
          {messages.length === 0 ? (
            <div className="h-full flex items-center justify-center text-zinc-500 text-sm">
              Waiting for messages...
            </div>
          ) : (
            <div className="flex flex-col gap-1">
              {messages.map((msg, i) => {
                const date = new Date(msg.timestamp);
                const timeStr = `${date.getHours().toString().padStart(2, '0')}:${date.getMinutes().toString().padStart(2, '0')}:${date.getSeconds().toString().padStart(2, '0')}.${date.getMilliseconds().toString().padStart(3, '0')}`;
                return (
                  <div key={`${msg.offset}-${i}`} className="flex items-center gap-3 px-3 py-1.5 hover:bg-zinc-800/50 rounded-lg transition-colors">
                    <span className="bg-zinc-800 text-zinc-400 text-xs px-1.5 py-0.5 rounded font-mono w-16 text-right shrink-0">
                      #{msg.offset}
                    </span>
                    <span className="text-zinc-500 text-xs font-mono shrink-0">
                      {timeStr}
                    </span>
                    <span className="text-zinc-400 text-xs font-mono truncate max-w-[100px] shrink-0">
                      {msg.key || '-'}
                    </span>
                    <span className="text-zinc-200 text-sm truncate flex-1 font-mono">
                      {msg.value}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
