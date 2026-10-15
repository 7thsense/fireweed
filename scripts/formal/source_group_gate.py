#!/usr/bin/env python3
"""Bounded source-plan, preparation and record-read gate; documentary Rust correspondence only."""
import hashlib
from pathlib import Path
import subprocess
import sys
import types

helper_path = Path(__file__).resolve().with_name('check_api007.py')
helper_bytes = helper_path.read_bytes()
helper = types.ModuleType('source_group_helpers')
helper.__file__ = str(helper_path)
sys.modules[helper.__name__] = helper
exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
helper.LOADED_SOURCES[str(helper_path)] = hashlib.sha256(helper_bytes).hexdigest()
model_path = helper_path.with_name('source_group_models.py')
model_bytes = model_path.read_bytes()
model = types.ModuleType('source_group_models')
model.__file__ = str(model_path)
sys.modules[model.__name__] = model
exec(compile(model_bytes, str(model_path), 'exec'), model.__dict__)
helper.LOADED_SOURCES[str(model_path)] = hashlib.sha256(model_bytes).hexdigest()

WITNESSES = {
    'mixed_check_corruption_ignored': ('mixed_check_corrupt','same','absent',False,'none',False),
    'state_metadata_only_check': ('state_check','same','absent',False,'field',False),
    'state_check_cannot_retarget': ('state_check','retarget','absent',False,'none',False),
    'foreign_equal_head_fields': ('mixed','cut_aba','absent',False,'none',False),
    'selected_overcount': ('selected_overcount','same','absent',False,'none',False),
    'complete_count': ('state_short','same','release',False,'none',False),
    'complete_chain': ('state_incomplete','same','release',False,'none',False),
    'future_sequence': ('future_sequence','same','absent',False,'none',False),
    'future_body': ('future_body','same','absent',False,'none',False),
    'future_state': ('future_state','same','absent',False,'none',False),
    'unique_release': ('mixed','same','release',False,'none',False),
    'unique_reclaim': ('mixed','same','reclaim',False,'none',False),
    'unique_swap': ('swap','same','swap',False,'none',False),
    'surviving_owner_conflict': ('mixed','same','conflict',False,'none',False),
    'missing_release_unknown': ('mixed','same','missing_release',False,'none',False),
    'business_rejection_precedes_release': ('mixed','same','missing_release',True,'none',False),
    'lease_expires_during_probe': ('mixed','same','missing_release',False,'none',True),
    'admission_failure': ('mixed','same','absent',False,'budget',False),
    'partial_cancel': ('mixed','same','release',False,'cancel',False),
    'probe_failure': ('mixed','same','release',False,'unique',False),
}

READ_WITNESSES = {
    'retry_after_provider_failure': ('same',2,4,10,'provider'),
    'cancel_then_retry': ('same',2,4,10,'cancel'),
    'reads_one_short': ('same',2,1,10,'success'),
    'bytes_one_short': ('same',2,4,4,'success'),
    'retained_aliases': ('same',2,4,10,'success'),
    'root_failure_work_spent': ('same',2,4,10,'root'),
    'missing_then_retry': ('same',2,4,10,'missing'),
    'malformed_then_retry': ('same',2,4,10,'malformed'),
}

def completed_record_witness(s, case):
    # Declarative event sets and balances from initial grants/outcome, independent
    # of transition flags, labels and the model's invariant implementation.
    if s.case != case or s.phase != 'done' or s.index != 1: return False
    short = case[2] < 2 or case[3] < 5
    grants = set() if short else ({1} if case[4] == 'missing' else {0,1})
    physical = set() if short else ({1} if case[4] in ('missing','root') else {0,1})
    aliases = set() if short else ({0,1} if case[4] == 'success' else {1})
    expected = {'plan':{0,1}, 'grant':grants, 'fund':physical,
        'hydrate':physical, 'retire':physical, 'release':physical,
        'alias':aliases, 'drop_alias':aliases,
        'cancel':{0} if case[4] == 'cancel' else set()}
    observed = {kind:{i for event,i in s.events if event == kind} for kind in expected}
    return (observed == expected and len(s.events) == sum(map(len, expected.values()))
        and (s.attempts,s.reads,s.body,s.semantic,s.credit,s.acknowledged)
            == (0,case[2]-2*len(grants),case[3]-5*len(grants),7,0,False))

def completed_genesis_witness(s, case):
    if s.phase != 'done' or (s.result,s.returned) != model.provision_expected(case): return False
    admitted = case.preflight and case.capacity > 0
    write = int(admitted and case.initial in ('empty','missing') and not case.known and not case.retry_winner)
    read = int(admitted and (case.initial == 'covered' or case.retry_winner or
        (write and case.accepted in ('a','b') and case.capacity >= 2)))
    history = model.provision_initial(case)
    if case.retry_winner or (write and case.accepted in ('a','b','delayed')):
        history = (model.GENESIS_B if case.retry_winner or case.accepted == 'b' else model.GENESIS_A,)
    if history and case.advance != 'none': history += (('later',),)
    snapshot = history[:-1] if case.advance == 'during' else history
    if (not admitted or case.initial == 'legacy' or (case.known and not history)
        or (write and case.accepted == 'delayed')): snapshot = ()
    # Full independent protocol words, including refusal reads and reply kind.
    events = ('preflight','open') if admitted else ()
    if admitted and case.initial not in ('covered','legacy') and not case.known:
        events += ('empty',)
        if case.retry_winner: events += ('acquire','observe-winner','capture')
        else:
            events += ('acquire','empty')*case.retries
            events += ('acquire','grant-write','possible','cas','reply-'+case.reply,'capture')
    elif admitted and case.initial == 'covered': events += ('capture',)
    if read:
        events += ('grant-read',)
        if case.corrupt != 'missing':
            events += ('point','record')
            if case.corrupt == 'none': events += ('verified',)
    if write and case.accepted == 'delayed': events += ('return-unknown','late-storage')
    fences = 1 if case.retry_winner and admitted else (case.retries+1)*write
    return (s.history == history and s.snapshot == snapshot and s.events == events
        and s.remaining == case.capacity-write-read and s.fences == fences)

def run(max_states=100000):
    specification = model.SourceGroup()
    report, graph = helper.explore(specification, max_states)
    if report['status'] != 'PASS' or not report['complete']: raise AssertionError('incomplete source-group baseline')
    states, edges, _ = graph
    report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in states], 'edges':edges})
    report['completed_witnesses'] = {}
    for name, case in WITNESSES.items():
        first = 'SelectCase-'+str(model.CASES.index(case))
        report['completed_witnesses'][name] = helper.witness(specification, graph, [first],
            lambda s, case=case: s.case == case and s.phase == 'done' and s.result == model.expected(case))
    controls = []
    for fault, expected in model.FAULTS.items():
        mutant = model.SourceGroup(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive source-group mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    publication = model.SourceGroupPublication()
    publication_report, publication_graph = helper.explore(publication, max_states)
    if publication_report['status'] != 'PASS' or not publication_report['complete']:
        raise AssertionError('incomplete publication boundary baseline')
    p_states, p_edges, _ = publication_graph
    publication_report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in p_states], 'edges':p_edges})
    publication_report['completed_witnesses'] = {}
    for i, case in enumerate(model.PUBLICATION_CASES):
        publication_report['completed_witnesses'][str(case)] = helper.witness(publication, publication_graph,
            ['SelectPublication-'+str(i)], lambda s, case=case: s.case == case and s.phase == 'done'
                and s.prepared == (case[1] == 'same' and case[2] == 'none'))
    for fault, expected in model.PUBLICATION_FAULTS.items():
        mutant = model.SourceGroupPublication(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive publication boundary mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    record = model.SourceRecordRead()
    record_report, record_graph = helper.explore(record, max_states)
    if record_report['status'] != 'PASS' or not record_report['complete']:
        raise AssertionError('incomplete source-record baseline')
    r_states, r_edges, _ = record_graph
    record_report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in r_states], 'edges':r_edges})
    record_report['completed_witnesses'] = {}
    for name, case in READ_WITNESSES.items():
        record_report['completed_witnesses'][name] = helper.witness(record, record_graph,
            ['SelectRead-'+str(model.READ_CASES.index(case))],
            lambda s, case=case: completed_record_witness(s, case))
    for fault, expected in model.READ_FAULTS.items():
        mutant = model.SourceRecordRead(fault)
        failure, _ = helper.explore(mutant, max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive source-record mutant '+fault)
        helper.replay(mutant, failure['trace'], expected)
        controls.append({'fault':fault, 'trace_sha256':helper.digest(failure['trace']), **failure})
    genesis = model.CoupledGenesisProvision()
    genesis_report, genesis_graph = helper.explore(genesis,max_states)
    if genesis_report['status'] != 'PASS' or not genesis_report['complete']:
        raise AssertionError('incomplete coupled Genesis baseline')
    g_states,g_edges,_ = genesis_graph
    genesis_report['graph_sha256'] = helper.digest({'states':[helper.render(s) for s in g_states],'edges':g_edges})
    genesis_report['completed_witnesses'] = {}
    for name,case in model.PROVISION_CASES.items():
        genesis_report['completed_witnesses'][name] = helper.witness(genesis,genesis_graph,
            ['SelectGenesis-'+name],lambda s,name=name,case=case:
                s.case == name and completed_genesis_witness(s,case))
    for fault,expected in model.PROVISION_FAULTS.items():
        mutant = model.CoupledGenesisProvision(fault)
        failure,_ = helper.explore(mutant,max_states)
        if failure['status'] != 'COUNTEREXAMPLE' or failure.get('property') != expected:
            raise AssertionError('insensitive coupled Genesis mutant '+fault)
        helper.replay(mutant,failure['trace'],expected)
        controls.append({'fault':fault,'trace_sha256':helper.digest(failure['trace']),**failure})
    return {'schema':'fireweed-api007-source-group-formal-v1', 'status':'PASS',
        'baseline':[report, publication_report, record_report, genesis_report], 'negative_controls':controls,
        'qualification':{'finite_abstract_safety':'recorded bounds only', 'production_mapping':'documentary_unexecuted',
            'excluded':['Rust refinement','exact allocation arithmetic','canonical decoding and hashes',
                        'publication/CAS','provider durability','performance','public QueueStore activation']},
        'production_mappings':[{'symbols':['load_group_preimage','QueueSourceGroupPreimage::plan','load_group_fields',
                'QueueSourceGroupPlan::encode','QueueSourceGroupPublication::record_for',
                'QueueNativeAcquired::prepare_group_publication'],
            'tests':['fields_cannot_recapture_foreign_cut_then_return_to_original_head',
                     'mixed_work_check_never_reads_another_keys_mutation_fields',
                     'atomic_state_and_next_work_match_shared_full_image_planner',
                     'source_bridge_complete_producer_matches_cold_oracle_real_s3'], 'status':'documentary_unexecuted'},
            {'symbols':['QueueNativeCaptured::read_source_record',
                        'QueueNativeSourceBudget::grant_record_attempt',
                        'QueueNativeSourceBudget::grant_hydration'],
             'tests':['pending_s3_source_record_cancellation_retains_frame_credit_and_spent_work'],
             'status':'documentary_unexecuted'}]}

def main(max_states, path, root):
    try:
        helper.validate_loaded_sources()
        before = helper.provenance(root)
        report = run(max_states)
        helper.validate_loaded_sources()
        if before != helper.provenance(root): raise ValueError('source changed during source-group verification')
        report['provenance'] = before
        report['provenance_sha256'] = helper.digest(before)
        report['loaded_sources'] = dict(helper.LOADED_SOURCES)
        helper.write_report(path, report)
        print(helper.canonical({'status':'PASS','models':4,'negative_controls':len(report['negative_controls'])}))
        return 0
    except (ValueError, AssertionError, helper.SearchLimit, OSError, subprocess.CalledProcessError) as error:
        failure = {'schema':'fireweed-api007-source-group-formal-v1','status':'FAIL','error':str(error)}
        helper.write_report(path, failure)
        print(helper.canonical(failure))
        return 1
