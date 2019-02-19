package common

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension


@ExtendWith(MockitoExtension::class)
@Tag("integration")
annotation class IntegrationTest