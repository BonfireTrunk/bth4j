package today.bonfire.oss.bth4j.executor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import today.bonfire.oss.bth4j.Concurrency;

class ResourceControllerTest {
    private long now;
    private ResourceController controller = new ResourceController(Concurrency.adaptive(8, 200), now);

    private ResourceController.Decision tick(double cpu, double memory) {
        now += TimeUnit.MILLISECONDS.toNanos(500);
        return controller.tick(new ResourceSampler.Sample(cpu, memory), 1, now);
    }

    private int grow(double cpu, double memory) {
        controller.claimed(8, 8);
        return tick(cpu, memory).limit();
    }

    private int ramp() {
        tick(0, 0.1);
        for (int i = 0; i < 4; i++) grow(0, 0.1);
        return 22;
    }

    @Test
    void fastRampReachesAboutOneHundredInElevenCreditedTicks() {
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
    void creditIsRequiredConsumedOnceAndClearedByPartialOrEmptyClaims() {
        tick(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(8);
        assertThat(grow(0, 0.1)).isEqualTo(10);
        assertThat(tick(0, 0.1).limit()).isEqualTo(10);
        controller.claimed(4, 4);
        controller.claimed(4, 3);
        assertThat(tick(0, 0.1).limit()).isEqualTo(10);
        controller.claimed(4, 4);
        controller.claimed(4, 0);
        assertThat(tick(0, 0.1).limit()).isEqualTo(10);
        controller.claimed(0, 0);
        assertThat(tick(0, 0.1).limit()).isEqualTo(10);
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
        controller = new ResourceController(Concurrency.adaptive(8, 200, 0.5, 0.8), now);
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
        controller = new ResourceController(Concurrency.adaptive(8, 200, 0.5, 0.8), now);
        ramp();
        for (int i = 0; i < 4; i++) tick(1, 0.1);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        assertThat(tick(1, 0.1).limit()).isEqualTo(18);
        assertThat(tick(1, 0.1).limit()).isEqualTo(15);
        for (int i = 0; i < 20; i++) tick(1, 0.1);
        assertThat(tick(1, 0.1).limit()).isEqualTo(8);
    }

    @Test
    void reductionsClearPreviouslyGrantedCredit() {
        ramp();
        controller.claimed(8, 8);
        for (int i = 0; i < 3; i++) tick(0, 0.85);
        for (int i = 0; i < 12; i++) tick(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(17);
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
    void memoryEmergencyGoesStraightToMinAndClearsCredit() {
        ramp();
        assertThat(grow(1, 0.95)).isEqualTo(8);
        for (int i = 0; i < 10; i++) tick(0, 0.1);
        assertThat(tick(0, 0.1).limit()).isEqualTo(8);
    }

    @Test
    void memoryReductionTakesPrecedenceOverCpuAndDoesNotLoseItsLongerCooldown() {
        controller = new ResourceController(Concurrency.adaptive(8, 200, 0.5, 0.8), now);
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
    void unknownMemoryAlsoBlocksAndDecaysAndNeedsTwoFreshSamples() {
        ramp();
        for (int i = 0; i < 9; i++) assertThat(grow(0, Double.NaN)).isEqualTo(22);
        assertThat(grow(0, Double.NaN)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(17);
        assertThat(grow(0, 0.1)).isEqualTo(22);
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
    void idleResetsAfterThirtySecondsButReservedSlotsAndActivityPreventIt() {
        ramp();
        long last = now;
        now += TimeUnit.SECONDS.toNanos(29);
        assertThat(controller.tick(new ResourceSampler.Sample(0, 0.1), 0, now).limit())
                .isEqualTo(22);
        controller.occupied(now);
        now += TimeUnit.SECONDS.toNanos(29);
        assertThat(controller.tick(new ResourceSampler.Sample(0, 0.1), 0, now).limit())
                .isEqualTo(22);
        now += TimeUnit.SECONDS.toNanos(1);
        assertThat(controller.tick(new ResourceSampler.Sample(0, 0.1), 0, now).limit())
                .isEqualTo(8);
        assertThat(now - last).isEqualTo(TimeUnit.SECONDS.toNanos(59));
    }

    @Test
    void idleResetAppliesEvenWhenPressureOrUnknownSignalsWouldAlsoReduce() {
        ramp();
        controller.tick(new ResourceSampler.Sample(1, 0.85), 0, now);
        now += TimeUnit.SECONDS.toNanos(30);
        assertThat(controller.tick(new ResourceSampler.Sample(1, 0.85), 0, now).limit())
                .isEqualTo(8);
    }

    @Test
    void clampsAtBothBoundsAndAlwaysRemovesASlotAboveMin() {
        controller = new ResourceController(Concurrency.adaptive(1, 3), now);
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
        controller = new ResourceController(Concurrency.adaptive(Integer.MAX_VALUE - 1, Integer.MAX_VALUE), now);
        tick(0, 0.1);
        assertThat(grow(0, 0.1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(grow(0, 0.1)).isEqualTo(Integer.MAX_VALUE);
        assertThat(tick(1, 0.95).limit()).isEqualTo(Integer.MAX_VALUE - 1);
    }

    @Test
    void lowTargetsScaleBothGrowthMargins() {
        controller = new ResourceController(Concurrency.adaptive(8, 200, 0.1, 0.1), now);
        tick(0.02, 0.02);
        assertThat(grow(0.02, 0.02)).isEqualTo(10);
        assertThat(grow(0.06, 0.06)).isEqualTo(11);
        assertThat(grow(0.09, 0.09)).isEqualTo(11);
    }

    @Test
    void permitDebtBlocksGrowthUntilOccupiedFallsToLimit() {
        ramp();
        controller.claimed(22, 22);
        now += TimeUnit.MILLISECONDS.toNanos(500);
        assertThat(controller.tick(new ResourceSampler.Sample(0, 0.1), 23, now).limit())
                .isEqualTo(22);
        assertThat(tick(0, 0.1).limit()).isEqualTo(28);
    }
}
