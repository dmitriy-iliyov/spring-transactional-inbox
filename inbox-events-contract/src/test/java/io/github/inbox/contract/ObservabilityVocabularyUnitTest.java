package io.github.inbox.contract;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ObservabilityVocabularyUnitTest {

    @Test
    @DisplayName("UT actions when listed across services should never repeat one value")
    void actions_whenListedAcrossServices_shouldBeUnique() {
        // when
        List<String> actions = allActions();

        // then
        assertThat(actions).doesNotHaveDuplicates();
    }

    @Test
    @DisplayName("UT actions when written should be lower-case dotted names a pipeline can filter on")
    void actions_whenWritten_shouldBeLowerCaseDottedNames() {
        // when / then
        assertThat(allActions()).allMatch(action -> action.matches("[a-z]+(\\.[a-z]+)+"));
    }

    @Test
    @DisplayName("UT fields when listed should never repeat a name and never reuse an action value")
    void fields_whenListed_shouldBeUniqueAndDistinctFromActions() {
        // when / then
        assertThat(ObservabilityVocabulary.FIELDS)
                .doesNotHaveDuplicates()
                .doesNotContainAnyElementsOf(allActions());
    }

    private static List<String> allActions() {
        return Stream.of(
                        ObservabilityVocabulary.ORDER_SERVICE_ACTIONS,
                        ObservabilityVocabulary.INBOX_ACTIONS,
                        ObservabilityVocabulary.LOYALTY_SERVICE_ACTIONS
                )
                .flatMap(List::stream)
                .toList();
    }
}
