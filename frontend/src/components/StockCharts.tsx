import React, { useEffect, useState } from 'react';
import { Card, Spin, Empty } from 'antd';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import { AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { getStockDaily } from '../api';
import type { Stock } from '../types';

interface StockChartsProps {
  stocks: Stock[];
  active: boolean; // Only fetch if active
}

const StockCharts: React.FC<StockChartsProps> = ({ stocks, active }) => {
  const [dataMap, setDataMap] = useState<Record<string, any[]>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});

  const fetchData = async () => {
    // Fetch in parallel
    stocks.forEach(async (stock) => {
      // If we already have data and just refreshing, maybe don't show full loading spinner?
      // But for now, let's just update quietly if data exists
      if (!dataMap[stock.symbol]) {
          setLoading(prev => ({ ...prev, [stock.symbol]: true }));
      }
      
      try {
        const res = await getStockDaily(stock.symbol);
        if (res.data.ok && res.data.data) {
          const newData = res.data.data;
          setDataMap(prev => ({ ...prev, [stock.symbol]: newData || [] }));
        }
      } catch (e) {
        console.error(e);
      } finally {
        setLoading(prev => ({ ...prev, [stock.symbol]: false }));
      }
    });
  };

  useEffect(() => {
    if (active) {
      fetchData();
      const interval = setInterval(fetchData, 60000);
      return () => clearInterval(interval);
    }
  }, [active, stocks]);

  if (!stocks.length) return <Empty description="暂无股票" />;

  return (
    <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))', gap: '16px' }}>
      {stocks.map(stock => {
        const data = dataMap[stock.symbol] || [];
        const lastPrice = data.length ? data[data.length - 1].close : 0;
        const firstPrice = data.length ? data[0].open : lastPrice; // Use open price of the day as base
        const change = lastPrice - firstPrice;
        const percent = firstPrice ? (change / firstPrice) * 100 : 0;
        const color = change >= 0 ? '#cf1322' : '#3f8600'; // Red up, Green down (China style)

        return (
          <Card key={stock.id} size="small" title={`${stock.name} (${stock.symbol})`} extra={
             data.length > 0 && (
                <span style={{ color, fontWeight: 'bold' }}>
                    {lastPrice.toFixed(2)} 
                    <span style={{ fontSize: 12, marginLeft: 4 }}>
                        {change >= 0 ? <ArrowUpOutlined /> : <ArrowDownOutlined />}
                        {Math.abs(percent).toFixed(2)}%
                    </span>
                </span>
             )
          }>
            <div style={{ height: 200 }}>
              {loading[stock.symbol] && !data.length ? (
                <div style={{ height: '100%', display: 'flex', justifyContent: 'center', alignItems: 'center' }}>
                    <Spin />
                </div>
              ) : (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={data}>
                    <defs>
                      <linearGradient id={`color${stock.symbol}`} x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor={color} stopOpacity={0.1}/>
                        <stop offset="95%" stopColor={color} stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <XAxis dataKey="time" minTickGap={30} tick={{ fontSize: 10 }} />
                    <YAxis domain={['auto', 'auto']} hide />
                    <Tooltip 
                        labelFormatter={(label) => label}
                        formatter={(value: any) => [Number(value).toFixed(2), '价格']}
                    />
                    <Area 
                        type="monotone" 
                        dataKey="close" 
                        stroke={color} 
                        fillOpacity={1} 
                        fill={`url(#color${stock.symbol})`} 
                        isAnimationActive={false}
                    />
                  </AreaChart>
                </ResponsiveContainer>
              )}
            </div>
          </Card>
        );
      })}
    </div>
  );
};

export default StockCharts;
