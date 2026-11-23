#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "postgres.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/faultinjector.h"
#include "utils/snapshot.h"
#include "utils/snapmgr.h"
#include "storage/fd.h"

#include "../sharedsnapshot.c"

static void
test_boundaries_of_CreateSharedSnapshotArray(void **state)
{
	/*
	 * MaxBackends is used to calculate NUM_SHARED_SNAPSHOT_SLOTS. Make
	 * it non-zero so that we actually allocate some local snapshot slots.
	 */
	MaxBackends = 2;

	SharedSnapshotStruct 	*fakeSharedSnapshotArray = NULL;
	LWLockPadded 			*fakeLockBase = NULL;
	bool found = false;

	expect_string(RequestNamedLWLockTranche, tranche_name, "SharedSnapshotLocks");
	expect_value(RequestNamedLWLockTranche, num_lwlocks, NUM_SHARED_SNAPSHOT_SLOTS);
	will_be_called(RequestNamedLWLockTranche);
	Size sharedSnapshotShmemSize = SharedSnapshotShmemSize();
	fakeSharedSnapshotArray = malloc(sharedSnapshotShmemSize);

	will_return(ShmemInitStruct, fakeSharedSnapshotArray);
	will_assign_value(ShmemInitStruct, foundPtr, found);
	expect_any_count(ShmemInitStruct, name, 1);
	expect_any_count(ShmemInitStruct, size, 1);
	expect_any_count(ShmemInitStruct, foundPtr, 1);

	expect_string(RequestNamedLWLockTranche, tranche_name, "SharedSnapshotLocks");
	expect_value(RequestNamedLWLockTranche, num_lwlocks, NUM_SHARED_SNAPSHOT_SLOTS);
	will_be_called(RequestNamedLWLockTranche);
	fakeLockBase = malloc(NUM_SHARED_SNAPSHOT_SLOTS * sizeof(LWLockPadded));
	will_return(GetNamedLWLockTranche, fakeLockBase);
	expect_any(GetNamedLWLockTranche, tranche_name);

	CreateSharedSnapshotArray();

	for (int i=0; i<sharedSnapshotArray->maxSlots; i++)
	{
		SharedSnapshotSlot *s = &sharedSnapshotArray->slots[i];

		/*
		 * Assert that each slot in SharedSnapshotStruct has an associated
		 * dynamically allocated LWLock.
		 */
		assert_true(s->slotLock == &fakeLockBase[i].lock);
		/*
		 * Assert that every slot xip array falls inside the boundaries of the
		 * allocated shared snapshot.
		 */
		assert_true((uint8_t *)s > (uint8_t *)fakeSharedSnapshotArray);
		assert_true((uint8_t *)s < (((uint8_t *)fakeSharedSnapshotArray) +
									sharedSnapshotShmemSize));
	}
}

static void
test_serialize_overflowed_snapshot_with_distributed_xids(void **state)
{
	SnapshotData original;
	TransactionId xip[] = {42};
	TransactionId subxip[] = {43};
	DistributedTransactionId dsxip[] = {UINT64CONST(0x123456789abcdef0)};
	TransactionId mappedLocalXids[] = {44};
	Size		snapshotSize;
	char	   *serialized;
	Snapshot	restored;

	MemSet(&original, 0, sizeof(original));
	original.snapshot_type = SNAPSHOT_MVCC;
	original.xmin = 40;
	original.xmax = 50;
	original.xip = xip;
	original.xcnt = lengthof(xip);
	original.subxip = subxip;
	original.subxcnt = lengthof(subxip);
	original.suboverflowed = true;
	original.haveDistribSnapshot = true;
	original.distribSnapshotWithLocalMapping.ds.count = lengthof(dsxip);
	original.distribSnapshotWithLocalMapping.ds.inProgressXidArray = dsxip;
	original.distribSnapshotWithLocalMapping.currentLocalXidsCount =
		lengthof(mappedLocalXids);
	original.distribSnapshotWithLocalMapping.inProgressMappedLocalXids =
		mappedLocalXids;

	snapshotSize = EstimateSnapshotSpace(&original);
	/*
	 * Leave room for the old, incorrect offset so the regression test can
	 * inspect the restored data without itself writing beyond the allocation.
	 */
	serialized = malloc(snapshotSize + sizeof(TransactionId));
	assert_true(serialized != NULL);
	memset(serialized, 0, snapshotSize + sizeof(TransactionId));
	SerializeSnapshot(&original, serialized);

	if (TopTransactionContext == NULL)
		TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												 "sharedsnapshot test transaction",
												 ALLOCSET_DEFAULT_SIZES);
	restored = RestoreSnapshot(serialized);
	assert_int_equal(restored->subxcnt, 0);
	assert_true(restored->suboverflowed);
	assert_int_equal(restored->distribSnapshotWithLocalMapping.ds.count, 1);
	assert_int_equal(restored->distribSnapshotWithLocalMapping.ds.inProgressXidArray[0],
				  dsxip[0]);
	assert_int_equal(restored->distribSnapshotWithLocalMapping.currentLocalXidsCount,
				  1);
	assert_int_equal(restored->distribSnapshotWithLocalMapping.inProgressMappedLocalXids[0],
				  mappedLocalXids[0]);

	pfree(restored);
	free(serialized);
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test_boundaries_of_CreateSharedSnapshotArray),
		unit_test(test_serialize_overflowed_snapshot_with_distributed_xids)
	};
	MemoryContextInit();
	InitFileAccess();
	return run_tests(tests);
}
