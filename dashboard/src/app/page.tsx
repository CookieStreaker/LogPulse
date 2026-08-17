'use client';

import React, { useEffect, useState, useRef } from 'react';
import { Database, FolderTree, MessagesSquare, Activity, Plus } from 'lucide-react';
import { StatCard } from './components/StatCard';
import { TopicTable, TopicData } from './components/TopicTable';
import { MessageStream, MessageData } from './components/MessageStream';
import { ConsumerGroups, ConsumerGroupData } from './components/ConsumerGroups';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8080';

interface Stats {
  broker_id: string;
  uptime_seconds: number;
  total_topics: number;
  total_partitions: number;
  total_messages: number;
  messages_per_sec: number;
  started_at: string;
}

export default function Dashboard() {
  const [stats, setStats] = useState<Stats | null>(null);
  const [topics, setTopics] = useState<TopicData[]>([]);
  const [consumers, setConsumers] = useState<ConsumerGroupData[]>([]);
  const [messages, setMessages] = useState<MessageData[]>([]);
  const [connected, setConnected] = useState(false);

  const [selectedTopic, setSelectedTopic] = useState<string>('');
  const [selectedPartition, setSelectedPartition] = useState<number>(0);
  
  // Track last seen offset to fetch only new messages
  const lastOffsetRef = useRef<number>(0);
  const selectedTopicRef = useRef<string>('');
  const selectedPartitionRef = useRef<number>(0);

  // Forms state
  const [isCreatingTopic, setIsCreatingTopic] = useState(false);
  const [newTopicName, setNewTopicName] = useState('');
  const [newTopicPartitions, setNewTopicPartitions] = useState(1);
  
  const [produceTopic, setProduceTopic] = useState('');
  const [produceKey, setProduceKey] = useState('');
  const [produceValue, setProduceValue] = useState('');

  useEffect(() => {
    selectedTopicRef.current = selectedTopic;
    selectedPartitionRef.current = selectedPartition;
  }, [selectedTopic, selectedPartition]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const [statsRes, topicsRes, consumersRes] = await Promise.all([
          fetch(`${API_URL}/api/stats`).catch(() => null),
          fetch(`${API_URL}/api/topics`).catch(() => null),
          fetch(`${API_URL}/api/consumers`).catch(() => null),
        ]);

        if (statsRes?.ok) {
          setStats(await statsRes.json());
          setConnected(true);
        } else {
          setConnected(false);
        }

        if (topicsRes?.ok) {
          const topicsData: TopicData[] = await topicsRes.json();
          setTopics(topicsData);
          
          if (!selectedTopicRef.current && topicsData.length > 0) {
            const firstTopic = topicsData[0];
            setSelectedTopic(firstTopic.name);
            if (firstTopic.partitions.length > 0) {
              setSelectedPartition(firstTopic.partitions[0].id);
            }
          }
          if (topicsData.length > 0 && !produceTopic) {
            setProduceTopic(topicsData[0].name);
          }
        }

        if (consumersRes?.ok) {
          setConsumers(await consumersRes.json());
        }

        // Fetch messages for selected topic/partition
        const currentTopic = selectedTopicRef.current;
        const currentPartition = selectedPartitionRef.current;
        
        if (currentTopic) {
          const msgRes = await fetch(
            `${API_URL}/api/messages/${currentTopic}/${currentPartition}?offset=${lastOffsetRef.current}&limit=50`
          ).catch(() => null);

          if (msgRes?.ok) {
            const msgData = await msgRes.json();
            if (msgData.messages && msgData.messages.length > 0) {
              setMessages(prev => {
                const newMessages = [...prev, ...msgData.messages];
                // Keep only last 100
                if (newMessages.length > 100) {
                  return newMessages.slice(newMessages.length - 100);
                }
                return newMessages;
              });
              
              const maxOffset = Math.max(...msgData.messages.map((m: any) => m.offset));
              lastOffsetRef.current = Math.max(lastOffsetRef.current, maxOffset + 1);
            }
          }
        }
      } catch (err) {
        setConnected(false);
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  const handleSelectTopic = (topic: string, partition: number) => {
    setSelectedTopic(topic);
    setSelectedPartition(partition);
    setMessages([]);
    lastOffsetRef.current = 0;
  };

  const handleCreateTopic = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!newTopicName) return;
    try {
      await fetch(`${API_URL}/api/topics`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newTopicName, partitions: newTopicPartitions })
      });
      setNewTopicName('');
      setNewTopicPartitions(1);
      setIsCreatingTopic(false);
    } catch (e) {
      console.error(e);
    }
  };

  const handleProduce = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!produceTopic || !produceValue) return;
    try {
      await fetch(`${API_URL}/api/produce`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ topic: produceTopic, key: produceKey, value: produceValue })
      });
      setProduceKey('');
      setProduceValue('');
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div className="min-h-screen p-6 pb-12 max-w-[1400px] mx-auto flex flex-col gap-8">
      {/* Header */}
      <header className="flex items-center justify-between border-b border-zinc-800 pb-4">
        <h1 className="text-xl font-semibold text-zinc-50 tracking-tight">Mini-Kafka</h1>
        <div className="flex items-center gap-2">
          <div className={`w-2.5 h-2.5 rounded-full ${connected ? 'bg-emerald-500' : 'bg-red-500'}`} />
          <span className="text-sm font-medium text-zinc-400">
            {connected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </header>

      {/* Stats Grid */}
      <section className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatCard icon={Database} label="Topics" value={stats?.total_topics ?? '-'} />
        <StatCard icon={FolderTree} label="Partitions" value={stats?.total_partitions ?? '-'} />
        <StatCard icon={MessagesSquare} label="Total Messages" value={stats?.total_messages ?? '-'} />
        <StatCard icon={Activity} label="Messages / sec" value={stats?.messages_per_sec ?? '-'} />
      </section>

      {/* Topics */}
      <section className="flex flex-col gap-4">
        <div className="flex justify-end">
          {!isCreatingTopic ? (
            <button 
              onClick={() => setIsCreatingTopic(true)}
              className="flex items-center gap-2 text-sm text-zinc-300 bg-zinc-800 hover:bg-zinc-700 px-3 py-1.5 rounded-lg transition"
            >
              <Plus size={16} /> Create Topic
            </button>
          ) : (
            <form onSubmit={handleCreateTopic} className="flex items-center gap-3 bg-zinc-900 border border-zinc-800 p-2 rounded-lg">
              <input 
                type="text" 
                placeholder="Topic name" 
                value={newTopicName}
                onChange={e => setNewTopicName(e.target.value)}
                className="bg-zinc-800 text-sm text-zinc-300 px-3 py-1.5 rounded outline-none focus:ring-1 focus:ring-zinc-600"
                required
              />
              <input 
                type="number" 
                min="1"
                placeholder="Partitions" 
                value={newTopicPartitions}
                onChange={e => setNewTopicPartitions(parseInt(e.target.value))}
                className="bg-zinc-800 text-sm text-zinc-300 px-3 py-1.5 rounded outline-none focus:ring-1 focus:ring-zinc-600 w-24"
                required
              />
              <button type="submit" className="bg-zinc-200 text-zinc-900 text-sm font-medium px-4 py-1.5 rounded hover:bg-zinc-300">
                Create
              </button>
              <button type="button" onClick={() => setIsCreatingTopic(false)} className="text-zinc-500 hover:text-zinc-300 text-sm px-2">
                Cancel
              </button>
            </form>
          )}
        </div>
        <TopicTable topics={topics} onSelectTopic={handleSelectTopic} />
      </section>

      {/* Producer Form (Simple) */}
      <section className="bg-zinc-900 border border-zinc-800 rounded-xl p-4 flex items-center gap-4">
        <h3 className="text-sm font-semibold text-zinc-300 shrink-0">Produce Message:</h3>
        <form onSubmit={handleProduce} className="flex items-center gap-3 w-full">
          <select 
            value={produceTopic} 
            onChange={e => setProduceTopic(e.target.value)}
            className="bg-zinc-800 text-sm text-zinc-300 px-3 py-1.5 rounded outline-none border border-zinc-700"
          >
            {topics.map(t => <option key={t.name} value={t.name}>{t.name}</option>)}
          </select>
          <input 
            type="text" 
            placeholder="Key (optional)" 
            value={produceKey}
            onChange={e => setProduceKey(e.target.value)}
            className="bg-zinc-800 text-sm text-zinc-300 px-3 py-1.5 rounded outline-none focus:ring-1 focus:ring-zinc-600 w-32 border border-zinc-700"
          />
          <input 
            type="text" 
            placeholder="Value" 
            value={produceValue}
            onChange={e => setProduceValue(e.target.value)}
            className="bg-zinc-800 text-sm text-zinc-300 px-3 py-1.5 rounded outline-none focus:ring-1 focus:ring-zinc-600 flex-1 border border-zinc-700"
            required
          />
          <button type="submit" className="bg-zinc-200 text-zinc-900 text-sm font-medium px-4 py-1.5 rounded hover:bg-zinc-300 shrink-0">
            Send
          </button>
        </form>
      </section>

      {/* Streams & Consumers Grid */}
      <section className="grid grid-cols-1 lg:grid-cols-2 gap-6 h-[500px]">
        <MessageStream 
          messages={messages} 
          selectedTopic={selectedTopic} 
          selectedPartition={selectedPartition} 
        />
        <ConsumerGroups groups={consumers} />
      </section>
    </div>
  );
}
