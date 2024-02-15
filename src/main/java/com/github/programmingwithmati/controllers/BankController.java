package com.github.programmingwithmati.controllers;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.services.BankBalanceService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bank-balance")
@RequiredArgsConstructor
public class BankController {

    private final BankBalanceService service;

    @GetMapping(value = "/{bank-balanceId}", produces = "application/json")
    public ResponseEntity<BankBalance> getBankBalanceById(@PathVariable("bank-balanceId") Long id) {
        return ResponseEntity.ok(service.getBankBalanceById(id));
    }

}
