import React, { useEffect, useRef } from 'react';
import * as echarts from 'echarts';

const EChart = ({ option, style, onEvents }) => {
  const domRef = useRef(null);
  const chartRef = useRef(null);

  useEffect(() => {
    if (!domRef.current) return;
    chartRef.current = echarts.init(domRef.current, undefined, { renderer: 'canvas' });
    if (option) {
      chartRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    }
    // bind events
    if (onEvents && chartRef.current) {
      Object.keys(onEvents).forEach((evt) => {
        chartRef.current.on(evt, onEvents[evt]);
      });
    }
    const handleResize = () => {
      chartRef.current && chartRef.current.resize();
    };
    window.addEventListener('resize', handleResize);
    return () => {
      window.removeEventListener('resize', handleResize);
      try {
        if (chartRef.current) {
          chartRef.current.dispose();
          chartRef.current = null;
        }
      } catch (e) {
        // swallow dispose errors
      }
    };
  }, []);

  useEffect(() => {
    if (chartRef.current && option) {
      chartRef.current.setOption(option, { notMerge: true, lazyUpdate: true });
    }
  }, [option]);

  return <div ref={domRef} style={style} />;
};

export default EChart;

