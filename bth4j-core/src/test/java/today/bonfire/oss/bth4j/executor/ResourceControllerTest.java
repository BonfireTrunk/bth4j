package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.Concurrency;
import today.bonfire.oss.bth4j.MemorySignal;

class ResourceControllerTest {
    private long now;
    private ResourceController controller = new ResourceController(Concurrency.adaptive(8, 200), now);
    private int limit = 8;

    private void configure(Concurrency.Adaptive settings) {
        controller = new ResourceController(settings, now);
        limit = settings.min();
    }

    private ResourceController.Decision tick(double cpu, double memory) {
        return tick(new ResourceSampler.Sample(cpu, memory), 1, 1);
    }

    private ResourceController.Decision tick(ResourceSampler.Sample sample, int occupied, int active) {
        now += TimeUnit.MILLISECONDS.toNanos(500);
        var decision = controller.tick(sample, occupied, active, now);
        limit = decision.limit();
        return decision;
    }

    private int grow(double cpu, double memory) {
        return grow(new ResourceSampler.Sample(cpu, memory));
    }

    private int grow(ResourceSampler.Sample sample) {
        controller.claimed(8, 8, now);
        return tick(sample, limit, 1).limit();
    }

    private int ramp() {
        return ramp(new ResourceSampler.Sample(0, 0.1));
    }

    private int ramp(ResourceSampler.Sample sample) {
        tick(sample, 1, 1);
        for (int i = 0; i < 4; i++) grow(sample);
        return limit;
    }

    @Test
    void fastRampReachesAboutOneHundredInElevenSaturatedTicksWithDemand() {
        tick(0.1, 0.1);
        int limit = 8;
        for (int i = 0; i < 11; i++) limit = grow(0.1, 0.1);
        assertThat(limit).isEqualTo(101);
    }

    @Test
    void slowBandGrowsOneAndHoldBandDoesNotGrow() {
        tick(0.8, 0.7);
        assertThat(grow(0.8, 0.7)).isEqualTo(9);
        assertThat(grow(0.88, 0.78)).isEqualTo(9);
    }

    @Test
    void bothRawAndSmoothedCpuMustHaveHeadroom() {
        tick(1, 0.1);
        assertThat(grow(0.1, 0.1)).isEqualTo(9);
        // Raw CPU alone would allow the fast band, but the EWMA still allows only the slow band.
        assertThat(tick(0, 0.1).smoothedCpu()).isEqualTo(0.58125);
        assertThat(grow(1, 0.1)).isEqualTo(9);
    }

    @Test
    void partialAndEmptyClaimsClearDemandEvidence() {
        tick(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(8);
        assertThat(grow(0, 0.1)).isEqualTo(10);
        assertThat(tick(0, 0.1).limit()).isEqualTo(10);
        controller.claimed(4, 4, now);
        controller.claimed(4, 3, now);
        var partial = tick(new ResourceSampler.Sample(0, 0.1), limit, 1);
        assertThat(partial.limit()).isEqualTo(11);
        assertThat(partial.reason()).isEqualTo("demand probe");
        controller.claimed(4, 4, now);
        controller.claimed(4, 0, now);
        assertThat(tick(new ResourceSampler.Sample(0, 0.1), limit, 1).limit()).isEqualTo(11);
        controller.claimed(0, 0, now);
        assertThat(tick(new ResourceSampler.Sample(0, 0.1), limit, 1).limit()).isEqualTo(11);
    }

    @Test
    void fullBatchDoesNotGrowWhenTheLimitHasFreeSlots() {
        configure(Concurrency.adaptive(100, 200));
        tick(0, 0.1);
        controller.claimed(32, 32, now);
        for (int i = 0; i < 12; i++) {
            assertThat(tick(new ResourceSampler.Sample(0, 0.1), 42, 10).limit()).isEqualTo(100);
        }
    }

    @Test
    void eachFullClaimAllowsOneStepAndStaleDemandFallsBackToProbing() {
        tick(0, 0.1);
        controller.claimed(8, 8, now);
        var sample = new ResourceSampler.Sample(0, 0.1);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(10);
        assertThat(tick(sample, limit, 1).reason()).isEqualTo("hold");
        controller.claimed(2, 2, now);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(13);
        for (int i = 0; i < 10; i++) tick(0, 0.1);
        var expired = tick(sample, limit, 1);
        assertThat(expired.limit()).isEqualTo(14);
        assertThat(expired.reason()).isEqualTo("demand probe");
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(14);
    }

    @Test
    void saturatedWithoutDemandProbesOneSlotAtMostEveryFiveSeconds() {
        var sample = new ResourceSampler.Sample(0, 0.1);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(8);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(9);
        for (int i = 0; i < 9; i++) assertThat(tick(sample, limit, 1).limit()).isEqualTo(9);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(10);
        for (int i = 0; i < 9; i++) assertThat(tick(sample, limit, 1).limit()).isEqualTo(10);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(11);
    }

    @Test
    void probesNeedHeadroomAndRespectTheCpuCooldown() {
        configure(Concurrency.adaptive(8, 200, 0.5, 0.8));
        ramp();
        for (int i = 0; i < 4; i++) tick(1, 0.1);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        controller.claimed(1, 0, now);
        var sample = new ResourceSampler.Sample(0, 0.1);
        for (int i = 0; i < 3; i++) assertThat(tick(sample, limit, 1).limit()).isEqualTo(18);
        var recovered = tick(sample, limit, 1);
        assertThat(recovered.limit()).isEqualTo(19);
        assertThat(recovered.reason()).isEqualTo("demand probe");
        for (int i = 0; i < 12; i++) {
            assertThat(tick(new ResourceSampler.Sample(0, 0.78), limit, 1).limit())
                    .isEqualTo(19);
        }
    }

    @Test
    void memoryNeedsOneSecondOfPressureReducesOncePerSecondAndCoolsDownForFiveSeconds() {
        ramp();
        assertThat(grow(0, 0.85)).isEqualTo(22);
        assertThat(tick(0, 0.85).limit()).isEqualTo(22);
        assertThat(tick(0, 0.85).limit()).isEqualTo(17);
        assertThat(tick(0, 0.85).limit()).isEqualTo(17);
        assertThat(tick(0, 0.85).limit()).isEqualTo(13);
        for (int i = 0; i < 9; i++) assertThat(grow(0, 0.1)).isEqualTo(13);
        assertThat(grow(0, 0.1)).isEqualTo(17);
    }

    @Test
    void cpuNeedsOneSecondAboveTheSmoothedThresholdAndCoolsDownForTwoSeconds() {
        configure(Concurrency.adaptive(8, 200, 0.5, 0.8));
        ramp();
        assertThat(tick(1, 0.1).limit()).isEqualTo(22);
        assertThat(tick(1, 0.1).limit()).isEqualTo(22);
        assertThat(tick(1, 0.1).limit()).isEqualTo(22);
        assertThat(tick(1, 0.1).limit()).isEqualTo(22);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        for (int i = 0; i < 3; i++) assertThat(grow(0, 0.1)).isEqualTo(18);
        assertThat(grow(0, 0.1)).isEqualTo(23);
    }

    @Test
    void cpuReductionsAreLimitedToOncePerSecondAndStopAtMin() {
        configure(Concurrency.adaptive(8, 200, 0.5, 0.8));
        ramp();
        for (int i = 0; i < 4; i++) tick(1, 0.1);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        assertThat(tick(1, 0.1).limit()).isEqualTo(15);
        for (int i = 0; i < 20; i++) tick(1, 0.1);
        assertThat(tick(1, 0.1).limit()).isEqualTo(8);
    }

    @Test
    void memoryReductionCooldownOutlastsDemandEvidenceThenAllowsAProbe() {
        ramp();
        controller.claimed(8, 8, now);
        for (int i = 0; i < 3; i++) tick(0, 0.85);
        var sample = new ResourceSampler.Sample(0, 0.1);
        for (int i = 0; i < 9; i++) assertThat(tick(sample, limit, 1).limit()).isEqualTo(17);
        var decision = tick(sample, limit, 1);
        assertThat(decision.limit()).isEqualTo(18);
        assertThat(decision.reason()).isEqualTo("demand probe");
    }

    @Test
    void briefPressureDoesNotAccumulateAcrossLowSamples() {
        ramp();
        tick(0, 0.85);
        tick(0, 0.1);
        tick(0, 0.85);
        assertThat(tick(0, 0.85).limit()).isEqualTo(22);
    }

    @Test
    void memoryEmergencyGoesStraightToMin() {
        ramp();
        assertThat(grow(1, 0.95)).isEqualTo(8);
        for (int i = 0; i < 10; i++) tick(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(8);
    }

    @Test
    void memoryReductionTakesPrecedenceOverCpuAndDoesNotLoseItsLongerCooldown() {
        configure(Concurrency.adaptive(8, 200, 0.5, 0.8));
        ramp();
        tick(1, 0.1);
        tick(1, 0.1);
        tick(1, 0.85);
        tick(1, 0.85);
        var decision = tick(1, 0.85);
        assertThat(decision.limit()).isEqualTo(17);
        assertThat(decision.reason()).isEqualTo("memory target");
        tick(1, 0.1);
        for (int i = 0; i < 8; i++) assertThat(grow(0, 0.1)).isLessThanOrEqualTo(14);
    }

    @Test
    void unknownCpuBlocksGrowthDecaysAfterFiveSecondsAndNeedsTwoFreshSamples() {
        ramp();
        for (int i = 0; i < 9; i++) assertThat(grow(Double.NaN, 0.1)).isEqualTo(22);
        assertThat(grow(Double.NaN, 0.1)).isEqualTo(17);
        assertThat(grow(Double.NaN, 0.1)).isEqualTo(17);
        assertThat(grow(Double.NaN, 0.1)).isEqualTo(13);
        assertThat(grow(0, 0.1)).isEqualTo(13);
        assertThat(grow(0, 0.1)).isEqualTo(17);
    }

    @Test
    void aSignalWithoutAValidReadingForFiveSecondsIsReportedOnceInTheControllersLife() {
        configure(Concurrency.adaptive(8, 200).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM));
        var lost = new ResourceSampler.Sample(Double.NaN, 0.1, Double.NaN);
        var reports = new ArrayList<List<String>>();
        for (int i = 0; i < 12; i++) reports.add(tick(lost, 1, 1).unavailable());

        assertThat(reports.subList(0, 9)).allSatisfy(r -> assertThat(r).isEmpty());
        assertThat(reports.get(9)).containsExactly("CPU load", "system memory usage");
        assertThat(reports.subList(10, 12)).allSatisfy(r -> assertThat(r).isEmpty());

        tick(new ResourceSampler.Sample(0, 0.1, 0.1), 1, 1);
        for (int i = 0; i < 12; i++) assertThat(tick(lost, 1, 1).unavailable()).isEmpty();
    }

    @Test
    void unknownMemoryAlsoBlocksAndDecaysAndNeedsTwoFreshSamples() {
        ramp();
        for (int i = 0; i < 9; i++) assertThat(grow(0, Double.NaN)).isEqualTo(22);
        assertThat(grow(0, Double.NaN)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(22);
    }

    @Test
    void heapEmergencyStillReducesWhenSystemMemoryIsUnavailable() {
        configure(Concurrency.adaptive(8, 200).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM));
        ramp(new ResourceSampler.Sample(0, 0.1, 0.1));
        var decision = tick(new ResourceSampler.Sample(0, 0.97, Double.NaN), limit, 1);
        assertThat(decision.limit()).isEqualTo(8);
        assertThat(decision.reason()).isEqualTo("memory emergency");
    }

    @Test
    void heapTargetStillReducesWhenSystemMemoryIsUnavailable() {
        configure(Concurrency.adaptive(8, 200).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM));
        ramp(new ResourceSampler.Sample(0, 0.1, 0.1));
        var sample = new ResourceSampler.Sample(0, 0.85, Double.NaN);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(22);
        assertThat(tick(sample, limit, 1).limit()).isEqualTo(22);
        var decision = tick(sample, limit, 1);
        assertThat(decision.limit()).isEqualTo(17);
        assertThat(decision.reason()).isEqualTo("memory target");
    }

    @Test
    void missingComponentBlocksGrowthAndDecaysDespiteValidCombinedMemory() {
        configure(Concurrency.adaptive(8, 200).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM));
        ramp(new ResourceSampler.Sample(0, 0.1, 0.1));
        var sample = new ResourceSampler.Sample(0, 0.1, Double.NaN);
        for (int i = 0; i < 9; i++) assertThat(grow(sample)).isEqualTo(22);
        var decision = tick(sample, limit, 1);
        assertThat(decision.limit()).isEqualTo(17);
        assertThat(decision.reason()).isEqualTo("unknown resource signal");
        var complete = new ResourceSampler.Sample(0, 0.1, 0.1);
        assertThat(grow(complete)).isEqualTo(17);
        assertThat(grow(complete)).isEqualTo(22);
    }

    @Test
    void componentAvailabilityIsTrackedIndependently() {
        configure(Concurrency.adaptive(8, 200).withMemorySignal(MemorySignal.HEAP_AND_SYSTEM));
        ramp(new ResourceSampler.Sample(0, 0.1, 0.1));
        for (int i = 0; i < 20; i++) {
            var sample = i % 2 == 0
                    ? new ResourceSampler.Sample(0, 0.1, Double.NaN)
                    : new ResourceSampler.Sample(0, Double.NaN, 0.1);
            assertThat(grow(sample)).isEqualTo(22);
        }
    }

    @Test
    void longPauseProducesOnlyOneReductionAndRequiresFreshSamples() {
        ramp();
        now += TimeUnit.MINUTES.toNanos(1);
        assertThat(grow(Double.NaN, 0.1)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(22);
    }

    @Test
    void idleResetsAfterThirtySecondsEvenWithReservedSlots() {
        ramp();
        now += TimeUnit.SECONDS.toNanos(29);
        assertThat(controller
                        .tick(new ResourceSampler.Sample(0, 0.1), 1, 0, now)
                        .limit())
                .isEqualTo(22);
        now += TimeUnit.SECONDS.toNanos(1);
        assertThat(controller
                        .tick(new ResourceSampler.Sample(0, 0.1), 1, 0, now)
                        .limit())
                .isEqualTo(8);
    }

    @Test
    void runningTasksPreventIdleResetAndStartTheTimerAtTheirLastActiveTick() {
        ramp();
        for (int i = 0; i < 80; i++) assertThat(tick(0, 0.1).limit()).isEqualTo(22);
        var sample = new ResourceSampler.Sample(0, 0.1);
        for (int i = 0; i < 59; i++) assertThat(tick(sample, 0, 0).limit()).isEqualTo(22);
        assertThat(tick(sample, 0, 0).limit()).isEqualTo(8);
    }

    @Test
    void nonemptyPartialClaimRestartsTheIdleTimerWithoutARunningTask() {
        ramp();
        var sample = new ResourceSampler.Sample(0, 0.1);
        for (int i = 0; i < 59; i++) tick(sample, 0, 0);
        controller.claimed(8, 1, now);
        for (int i = 0; i < 59; i++) assertThat(tick(sample, 0, 0).limit()).isEqualTo(22);
        assertThat(tick(sample, 0, 0).limit()).isEqualTo(8);
    }

    @Test
    void idleResetAppliesEvenWhenPressureOrUnknownSignalsWouldAlsoReduce() {
        ramp();
        controller.tick(new ResourceSampler.Sample(1, 0.85), 0, 0, now);
        now += TimeUnit.SECONDS.toNanos(30);
        assertThat(controller
                        .tick(new ResourceSampler.Sample(1, 0.85), 0, 0, now)
                        .limit())
                .isEqualTo(8);
    }

    @Test
    void clampsAtBothBoundsAndAlwaysRemovesASlotAboveMin() {
        configure(Concurrency.adaptive(1, 3));
        tick(0, 0.1);
        for (int i = 0; i < 5; i++) grow(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(3);
        tick(0, 0.85);
        tick(0, 0.85);
        assertThat(tick(0, 0.85).limit()).isEqualTo(2);
        for (int i = 0; i < 20; i++) assertThat(tick(1, 1).limit()).isEqualTo(1);
    }

    @Test
    void growthArithmeticCannotOverflow() {
        configure(Concurrency.adaptive(Integer.MAX_VALUE - 1, Integer.MAX_VALUE));
        tick(0, 0.1);
        assertThat(grow(0, 0.1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(grow(0, 0.1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(tick(1, 0.95).limit()).isEqualTo(Integer.MAX_VALUE - 1);
    }

    @Test
    void lowTargetsScaleBothGrowthMargins() {
        configure(Concurrency.adaptive(8, 200, 0.1, 0.1));
        tick(0.02, 0.02);
        assertThat(grow(0.02, 0.02)).isEqualTo(10);
        assertThat(grow(0.06, 0.06)).isEqualTo(11);
        assertThat(grow(0.09, 0.09)).isEqualTo(11);
    }

    @Test
    void permitDebtCountsAsSaturatedAndAllowsGrowthWithFreshDemand() {
        ramp();
        controller.claimed(22, 22, now);
        assertThat(tick(new ResourceSampler.Sample(0, 0.1), 23, 23).limit()).isEqualTo(28);
    }
}
