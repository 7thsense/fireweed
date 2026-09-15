from pathlib import Path
import hashlib,json
repo=Path('/home/erik/Projects/fireweed')
binary=Path('/tmp/fireweed-stack-symfs/home/erik/Projects/fireweed/target/release/fireweed-workload')
assert hashlib.sha256(binary.read_bytes()).hexdigest()=='7297c9fc18783b0c54eafc811cb0faa2ed91d20cfd12ac9643c34117571d1b0a'
s=(repo/'scripts/perf/workflow-capacity.py').read_text()
s=s.replace('repo = Path(__file__).resolve().parents[2]',f'repo = Path({str(repo)!r})').replace('binary = repo / "target/release/fireweed-workload"',f'binary = Path({str(binary)!r})')
p=Path('/tmp/fireweed-baseline-capacity.py');p.write_text(s)
s=Path('/tmp/fireweed-run-campaign-shard-candidate.py').read_text().replace("'scripts/perf/workflow-capacity.py'",repr(str(p))).replace("repo/'target/release/fireweed-workload'",f'Path({str(binary)!r})')
Path('/tmp/fireweed-run-baseline-campaign.py').write_text(s)
Path('/tmp/fireweed-baseline-comparison-provenance.json').write_text(json.dumps({'purpose':'unqualified same-workload binary comparison; repository head is harness head, runtime source is explicit below','runtime_source_commit':'8a12e2de','binary_sha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'baseline_is_exact_archived_pre_cache_cli':True,'harness_changes':['repository root constant because wrapper lives in /tmp','binary path selects preserved baseline; all workload/measurement code unchanged'],'wrapper_sha256':hashlib.sha256(p.read_bytes()).hexdigest()},indent=2)+'\n')
