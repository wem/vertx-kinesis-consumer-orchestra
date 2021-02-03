package ch.sourcemotion.vertx.kinesis.consumer.orchestra.impl.ext

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule

internal fun ObjectMapper.registerKinesisOrchestraModules(): ObjectMapper =
    registerKotlinModule().registerModule(JavaTimeModule())
