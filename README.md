# Real-Time Streaming Analytics for International Expansion and Demand–Supply Optimization in On-Demand Mobility

## 📌 Project Overview

This project develops an end-to-end **real-time streaming analytics solution for an on-demand mobility business**.

The objective is to help a mobility startup identify **demand–supply imbalances, operational problems, and potential international expansion markets** using continuously generated mobility events.

Traditional batch-based analysis may identify problems only after they have occurred. In an on-demand mobility business, demand, driver availability, waiting time, cancellations, and customer experience can change rapidly. Therefore, real-time streaming analytics can help the business respond faster to operational issues and make better expansion decisions.

The project uses **Python, Apache Kafka, MySQL, and Grafana** to build a complete streaming data pipeline.

---

## 🎯 Business Problem

An on-demand mobility startup wants to expand internationally but needs to determine:

- Which markets should be prioritized for expansion?
- Where are demand and driver supply mismatched?
- Which cities have the highest demand?
- Where are driver shortages affecting service quality?
- Which cities have high cancellation rates?
- Where are customer ETAs increasing?
- Which markets generate higher revenue?
- Where should additional driver supply be allocated?

The project addresses these questions through real-time streaming analytics.

---

## 💡 Proposed Solution

We developed an end-to-end streaming analytics pipeline:

```text
Python Data Generator
        ↓
Kafka Producer
        ↓
Apache Kafka
        ↓
Kafka Topic
mobility_streaming_events
        ↓
Kafka Consumer
        ↓
Real-Time Processing
        ↓
MySQL Database
        ↓
Grafana Dashboard
        ↓
Business Insights & Decisions
