# CMSI 694 Capstone — Chatbot

**Course:** CMSI 694 Graduate Capstone Project — Spring 2026  
**Instructor:** Francis Nickels (francis.nickels@lmu.edu)  
**Team Members:** Vraj Patel, Jinil Patel  

## Overview
Our primary project idea is a **scheduling chatbot for university event Operations Assistants (undergraduate student workers)**. University events depend on student assistants whose availability changes often due to academic and personal commitments. Managing schedules manually can be time-consuming and becomes especially difficult when handling last-minute call-outs or event changes.

This project proposes a chatbot-driven system where student assistants can **submit availability, view assigned shifts, request shift changes/swaps, and report call-outs**. An administrator (or scheduling lead) can **create events, define staffing requirements, and approve or override schedule updates**. The goal is to streamline staffing coordination and reduce scheduling friction through a simple conversational interface.

## MVP Goals (initial)
- Collect and store Operations Assistant availability
- Allow assistants to view assigned shifts and upcoming event coverage
- Support basic schedule change requests (add/drop/swap) with clear confirmations
- Support call-out reporting and suggest replacement options based on availability
- Provide an admin flow to create events, set staffing needs, and update the schedule

## Backup Ideas
If the scheduling chatbot scope changes, we may pivot to one of these alternatives:

- **AI-Based Academic Advisor Chatbot:** A rule-driven chatbot that helps students with course selection, prerequisite checks, and degree progress guidance (as a supplement to human advisors).
- **Smart Campus Helpdesk Chatbot:** A chatbot that answers common questions about campus services (hours, locations, policies) using curated information and escalates complex issues appropriately.


# OA Scheduler (Sprint 1)

## Setup
```bash
python -m venv .venv
# activate venv
pip install -r requirements.txt
