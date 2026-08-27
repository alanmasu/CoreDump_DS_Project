# CoreDump educational subsystem reports

This directory contains long-form study reports for the CoreDump distributed-systems project. The reports are deliberately more detailed than the final three-to-six-page exam report. Their purpose is to teach the codebase and preserve evidence that a later, separate writing pass can condense.

The final exam report is intentionally **not** included.

This is the expanded deep study edition. Every subsystem report is at least ten
A4 pages and combines a codebase map with beginner explanations,
“code-microscope” walkthroughs, concrete failure or timing examples, six
rendered Mermaid study diagrams, and practice questions. The wording is
intentionally instructional so a student can read one report without already
knowing Akka or distributed-systems terminology.

## Reading order

1. `00_requirements_and_system_contract.pdf`
2. `01_actor_architecture_and_initialization.pdf`
3. `02_communication_fifo_and_time.pdf`
4. `03_transaction_message_and_dispatch_framework.pdf`
5. `04_replicated_state_epochs_and_history.pdf`
6. `05_client_api_and_request_manager.pdf`
7. `06_read_protocol.pdf`
8. `07_write_request_orchestration.pdf`
9. `08_quorum_update_and_total_order_broadcast.pdf`
10. `09_heartbeat_and_silent_failure_detection.pdf`
11. `10_crash_model_and_fault_injection.pdf`
12. `11_ring_election_protocol.pdf`
13. `12_recovery_synchronization_and_epoch_transition.pdf`
14. `13_observability_testing_and_verification.pdf`
15. `14_end_to_end_correctness_and_corner_cases.pdf`

Editable Markdown lives in `sources/`. Run `./reports/build_reports.sh` from anywhere in the repository to regenerate the PDFs. The build renders Mermaid fences with `mmdc`, parses Markdown with `cmark`, and lays out PDFs with the local `render_reports.py` ReportLab renderer. Generated HTML and Mermaid PNGs are kept in `reports/generated/` so rendering problems can be inspected without editing the PDFs.

The current deep edition is 15 PDFs, at least 10 pages per report, and 90
rendered Mermaid diagrams. The build fails if any PDF falls below the page
minimum. PDF validation checks page metadata and extractable text;
representative pages are also rendered to images to check diagrams, callouts,
tables, and code blocks.

## Provenance rule

This repository is not yet a single integrated implementation. Every report names the Git ref and commit it inspected and distinguishes:

- **Implemented on the inspected ref**
- **Implemented on another feature branch**
- **Partially implemented**
- **Required by the specification but not implemented**

That distinction must be preserved when these reports are used as input to another chat.
