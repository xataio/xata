package billing

import (
	"context"
	"time"

	authv1 "xata/gen/proto/auth/v1"
)

//go:generate go run github.com/vektra/mockery/v2 --output billingmock --outpkg billingmock --with-expecter --name Client

const (
	TrialCreditExpiryDays      = 60
	CreditStatusActive         = "active"
	CreditStatusPendingPayment = "pending_payment"
)

type CollectionMethod string

const (
	CollectionMethodUnknown             CollectionMethod = "unknown"
	CollectionMethodStripePaymentMethod CollectionMethod = "stripe_payment_method"
	CollectionMethodMarketplace         CollectionMethod = "marketplace"
	CollectionMethodBankTransfer        CollectionMethod = "bank_transfer"
)

type SubscriptionStatus string

const SubscriptionStatusActive SubscriptionStatus = "active"

type Subscription struct {
	ID                 string
	Status             SubscriptionStatus
	AutoCollection     bool
	NetTerms           int
	DefaultInvoiceMemo string
}

type OrbBankTransferOptions struct {
	ExternalCustomerID string
	StripeCustomerID   string
	SubscriptionID     string
	NetTerms           int
	InvoiceMemo        string
}

type Credit struct {
	ID                    string
	Amount                float64
	MaximumInitialBalance float64
	EffectiveDate         time.Time
	ExpiryDate            time.Time
	Status                string
}

func (c *Credit) IsExpired() bool {
	// The orb client uses a zero time to indicate no expiry
	return !c.ExpiryDate.IsZero() && time.Now().After(c.ExpiryDate)
}

func (c Credit) IsActive(now time.Time) bool {
	if c.Status != CreditStatusActive || c.Amount <= 0 {
		return false
	}
	if !c.EffectiveDate.IsZero() && c.EffectiveDate.After(now) {
		return false
	}
	return c.ExpiryDate.IsZero() || c.ExpiryDate.After(now)
}

type PaymentMethodCard struct {
	Brand       string
	Last4       string
	ExpiryMonth int
	ExpiryYear  int
}

type PaymentMethod struct {
	Card PaymentMethodCard
}

type Customer struct {
	ID                   string
	CustomerID           string
	CustomerExternalID   string
	Name                 string
	Email                string
	PaymentProvider      string
	PaymentProviderID    string
	AutoCollection       bool
	Subscriptions        []Subscription
	DefaultPaymentMethod *PaymentMethod
	// HasValidPaymentMethod indicates Stripe has a supported default payment method. Use CanCollectPayment for collection decisions because marketplace billing does not require one.
	HasValidPaymentMethod bool
	Organization          *authv1.Organization
}

func (c *Customer) CollectionMethod() CollectionMethod {
	if c == nil {
		return CollectionMethodUnknown
	}
	// Auth normalizes unsupported collection methods to "unknown" before returning organizations, so this cast only produces declared CollectionMethod values.
	return CollectionMethod(c.Organization.GetBillingCollectionMethod())
}

func (c *Customer) CanCollectPayment() bool {
	method := c.CollectionMethod()
	return method == CollectionMethodMarketplace ||
		method == CollectionMethodBankTransfer ||
		(method == CollectionMethodStripePaymentMethod && c.HasValidPaymentMethod)
}

// CustomerWithCredits contains explicitly loaded credits, separate from customer hydration.
type CustomerWithCredits struct {
	*Customer
	Credits []Credit
}

func (c *CustomerWithCredits) CurrentActiveCredit() float64 {
	var credit float64
	for _, credits := range c.Credits {
		if !credits.IsExpired() && credits.Amount > 0 {
			credit += float64(credits.Amount)
		}
	}

	return credit
}

func (c *CustomerWithCredits) TotalLifetimeCredits() float64 {
	var total float64
	for _, credit := range c.Credits {
		total += credit.MaximumInitialBalance
	}
	return total
}

func (c *CustomerWithCredits) ActiveCredits(now time.Time) []Credit {
	if c == nil {
		return nil
	}

	active := make([]Credit, 0, len(c.Credits))
	for _, credit := range c.Credits {
		if credit.IsActive(now) {
			active = append(active, credit)
		}
	}
	return active
}

func (c *CustomerWithCredits) TotalActiveCredits(now time.Time) float64 {
	if c == nil {
		return 0
	}

	var total float64
	for _, credit := range c.Credits {
		if credit.IsActive(now) {
			total += credit.Amount
		}
	}
	return total
}

func (c *CustomerWithCredits) LastExpiry() *time.Time {
	if c == nil {
		return nil
	}

	var last *time.Time
	for _, credit := range c.Credits {
		last = laterExpiry(last, credit.ExpiryDate)
	}
	return last
}

func (c *CustomerWithCredits) LastExpiryWithBalance() *time.Time {
	if c == nil {
		return nil
	}

	var last *time.Time
	for _, credit := range c.Credits {
		if credit.Amount > 0 {
			last = laterExpiry(last, credit.ExpiryDate)
		}
	}
	return last
}

func (c *CustomerWithCredits) LastActiveCreditExpiry(now time.Time) *time.Time {
	if c == nil {
		return nil
	}

	var last *time.Time
	for _, credit := range c.Credits {
		if credit.IsActive(now) {
			last = laterExpiry(last, credit.ExpiryDate)
		}
	}
	return last
}

func laterExpiry(current *time.Time, candidate time.Time) *time.Time {
	if candidate.IsZero() || (current != nil && !candidate.After(*current)) {
		return current
	}
	return &candidate
}

type InvoiceAutoCollection struct {
	NumAttempts   int
	NextAttemptAt *time.Time
}

type InvoiceStatus string

const (
	InvoiceStatusIssued InvoiceStatus = "issued"
	InvoiceStatusPaid   InvoiceStatus = "paid"
)

type Invoice struct {
	ID                 string
	InvoiceNumber      string
	AmountDue          float64
	AmountDueCents     int64
	Total              float64
	Currency           string
	Status             InvoiceStatus
	WillAutoIssue      bool
	InvoiceDate        time.Time
	BillingPeriodStart time.Time
	DueDate            time.Time
	IssuedAt           time.Time
	PaidAt             time.Time
	InvoicePDF         *string
	AutoCollection     InvoiceAutoCollection
}

type InvoiceListOptions struct {
	Cursor   string
	Limit    int
	Statuses []InvoiceStatus
}

type InvoiceSummary struct {
	ID                 string
	CustomerExternalID string
	Status             InvoiceStatus
	IssuedAt           time.Time
}

type InvoiceSummaryListOptions struct {
	ExternalCustomerID string
	Limit              int
	Status             InvoiceStatus
	InvoiceDateGte     time.Time
	InvoiceDateLte     time.Time
}

type MarkInvoicePaidOptions struct {
	InvoiceID           string
	PaymentReceivedDate time.Time
	ExternalID          string
	IdempotencyKey      string
	Note                string
}

type InvoicesPage struct {
	Data       []Invoice
	HasMore    bool
	NextCursor *string
}

type UpcomingInvoice struct {
	CreatedAt        time.Time
	AmountDue        float64
	Currency         string
	TargetDate       time.Time
	Subtotal         float64
	Total            float64
	HostedInvoiceURL *string
}

type OrbCustomerMetadata struct {
	Marketplace string
}

type CreateCustomerOptions struct {
	Name                  string
	Email                 string
	ExternalCustomerID    string
	OrganizationsCount    int
	Metadata              OrbCustomerMetadata
	SubscriptionStartDate *time.Time
}

type CreateCustomerResult struct {
	CreditAmount int
}

func (m OrbCustomerMetadata) Values() map[string]string {
	if m.Marketplace == "" {
		return nil
	}
	return map[string]string{"marketplace": m.Marketplace}
}

type StripeCustomer struct {
	ID             string
	OrganizationID string
}

type BankTransferAddress struct {
	Line1      string `json:"line1"`
	Line2      string `json:"line2,omitempty"`
	City       string `json:"city"`
	State      string `json:"state"`
	PostalCode string `json:"postal_code"`
	Country    string `json:"country"`
}

type FindStripeBankTransferCustomerOptions struct {
	OrganizationID         string
	LinkedStripeCustomerID string
}

type ConfigureStripeBankTransferCustomerOptions struct {
	OrganizationID   string
	StripeCustomerID string
	Email            string
	Address          BankTransferAddress
}

type StripeFundingInstructions struct {
	Currency          string
	Network           string
	AccountHolderName string
	BankName          string
	AccountNumber     string
	RoutingNumber     string
	AccountType       string
}

type StripeBankTransferResult struct {
	CustomerID          string
	Created             bool
	FundingInstructions StripeFundingInstructions
}

type StripeCashBalanceTransaction struct {
	ID                   string
	CustomerID           string
	Currency             string
	NetAmount            float64
	NetAmountCents       int64
	CreatedAt            time.Time
	FundedByBankTransfer bool
}

type BankTransferReconciliationOutcome string

const (
	BankTransferReconciliationIgnored   BankTransferReconciliationOutcome = "ignored"
	BankTransferReconciliationManual    BankTransferReconciliationOutcome = "manual"
	BankTransferReconciliationRetryable BankTransferReconciliationOutcome = "retryable_error"
	BankTransferReconciliationSucceeded BankTransferReconciliationOutcome = "succeeded"
)

type BankTransferReconciliationReason string

const (
	BankTransferReasonNone                  BankTransferReconciliationReason = ""
	BankTransferReasonNotFunded             BankTransferReconciliationReason = "not_funded_by_bank_transfer"
	BankTransferReasonCustomerNotMapped     BankTransferReconciliationReason = "customer_not_mapped"
	BankTransferReasonNoIssuedInvoice       BankTransferReconciliationReason = "no_issued_invoice"
	BankTransferReasonMultipleInvoices      BankTransferReconciliationReason = "multiple_issued_invoices"
	BankTransferReasonInvoicePageIncomplete BankTransferReconciliationReason = "issued_invoice_page_incomplete"
	BankTransferReasonCurrencyMismatch      BankTransferReconciliationReason = "currency_mismatch"
	BankTransferReasonAmountMismatch        BankTransferReconciliationReason = "amount_mismatch"
	BankTransferReasonInvoiceNotPayable     BankTransferReconciliationReason = "invoice_not_payable"
	BankTransferReasonPaymentNotSucceeded   BankTransferReconciliationReason = "payment_intent_not_succeeded"
	BankTransferReasonProviderFailure       BankTransferReconciliationReason = "provider_failure"
)

type BankTransferReconciliation struct {
	StripeCustomerID      string
	StripeTransactionID   string
	Transaction           *StripeCashBalanceTransaction
	Customer              *Customer
	IssuedInvoices        []Invoice
	HasMoreIssuedInvoices bool
	MatchedInvoice        *Invoice
	PaymentIntentID       string
	PaymentIntentStatus   string
	InvoiceMarkedPaid     bool
	Outcome               BankTransferReconciliationOutcome
	Reason                BankTransferReconciliationReason
}

type CheckoutSession struct {
	URL string
}

type PaymentMethodSession struct {
	URL string
}

type StripePaymentMethodCard struct {
	ID    string
	Last4 string
}

type Client interface {
	// CreateCustomer creates a new customer in the billing system
	CreateCustomer(ctx context.Context, opts CreateCustomerOptions) (CreateCustomerResult, error)
	// FetchCustomerWithCreditsByOrbID retrieves a customer using its Orb internal customer ID.
	FetchCustomerWithCreditsByOrbID(ctx context.Context, customerID string) (*CustomerWithCredits, error)
	// FetchCustomerByOrbID retrieves a customer without loading credits.
	FetchCustomerByOrbID(ctx context.Context, customerID string) (*Customer, error)
	// LoadCustomerCredits loads credits without refetching or mutating the customer.
	LoadCustomerCredits(ctx context.Context, customer *Customer) (*CustomerWithCredits, error)
	// FetchCustomer retrieves a customer by organization ID without loading credits.
	FetchCustomer(ctx context.Context, organizationID string) (*Customer, error)
	// FetchCustomerWithCredits retrieves a customer by organization ID with default payment method details and credits.
	FetchCustomerWithCredits(ctx context.Context, organizationID string) (*CustomerWithCredits, error)
	// ConfigureOrbCustomerForBankTransfers configures an Orb customer and subscription for manual collection through Stripe.
	ConfigureOrbCustomerForBankTransfers(ctx context.Context, opts OrbBankTransferOptions) error
	UpdateOrbCustomerEmail(ctx context.Context, organizationID, email string) (*CustomerWithCredits, error)
	ListInvoices(ctx context.Context, externalCustomerID string, opts InvoiceListOptions) (*InvoicesPage, error)
	// ListInvoiceSummaries retrieves all matching summaries; opts.Limit controls page size, not total results.
	ListInvoiceSummaries(ctx context.Context, opts InvoiceSummaryListOptions) ([]InvoiceSummary, error)
	// FetchUpcomingInvoice fetches the next invoice for the customer's active subscription.
	// Customers are expected to have at most one active subscription.
	FetchUpcomingInvoice(ctx context.Context, externalCustomerID string) (*UpcomingInvoice, error)
	// ListCustomersWithCreditsCreatedAfter retrieves customer records created at or after the given time.
	ListCustomersWithCreditsCreatedAfter(ctx context.Context, createdAfter time.Time) ([]*CustomerWithCredits, error)
	// FetchStripeCustomer retrieves a Stripe customer by their Stripe customer ID.
	FetchStripeCustomer(ctx context.Context, stripeCustomerID string) (*StripeCustomer, error)
	// FindExistingStripeCustomerForBankTransfers returns a safe existing Stripe customer ID without writes.
	FindExistingStripeCustomerForBankTransfers(ctx context.Context, opts FindStripeBankTransferCustomerOptions) (stripeCustomerID string, err error)
	// ConfigureStripeCustomerForBankTransfers creates or configures a Stripe customer and ensures USD ACH funding instructions.
	ConfigureStripeCustomerForBankTransfers(ctx context.Context, opts ConfigureStripeBankTransferCustomerOptions) (StripeBankTransferResult, error)
	// ReconcileBankTransfer reconciles a funded Stripe cash-balance transaction with one matching Orb invoice.
	ReconcileBankTransfer(ctx context.Context, stripeCustomerID, transactionID string) (BankTransferReconciliation, error)
	// FetchPaymentIntentPaymentMethodID retrieves the payment method attached to a Stripe payment intent.
	FetchPaymentIntentPaymentMethodID(ctx context.Context, paymentIntentID string) (string, error)
	// FetchSetupIntentPaymentMethodID retrieves the payment method attached to a Stripe setup intent.
	FetchSetupIntentPaymentMethodID(ctx context.Context, setupIntentID string) (string, error)
	// FetchStripePaymentMethodCard retrieves Stripe card details for a payment method.
	FetchStripePaymentMethodCard(ctx context.Context, paymentMethodID string) (*StripePaymentMethodCard, error)
	// FetchInvoice retrieves an orb invoice. invoiceID is an Orb internal invoice id
	FetchInvoice(ctx context.Context, invoiceID string) (*Invoice, error)
	// EnsureDefaultPaymentMethod sets paymentMethodID as the Stripe customer's default payment method
	// if it is not already set.
	EnsureDefaultPaymentMethod(ctx context.Context, stripeCustomerID, paymentMethodID string) error
	// SetOrbCustomerStripeChargeProvider configures the Orb customer to charge the Stripe customer.
	SetOrbCustomerStripeChargeProvider(ctx context.Context, externalCustomerID, stripeCustomerID string) error
	// RefundPaymentIntent creates a Stripe refund for a payment intent.
	RefundPaymentIntent(ctx context.Context, paymentIntentID string, metadata map[string]string, idempotencyKey string) error
	// HasValidDefaultPaymentMethod returns true if the Stripe customer has a valid default payment method.
	HasValidDefaultPaymentMethod(ctx context.Context, stripeCustomerID string) (bool, error)
	// VoidInvoice voids an invoice in the billing system.
	VoidInvoice(ctx context.Context, invoiceID string) error
	// MarkInvoicePaid marks an issued invoice as paid in the billing system.
	MarkInvoicePaid(ctx context.Context, opts MarkInvoicePaidOptions) error
	// CountPendingInvoices returns the number of issued (unpaid) invoices that have had at least one payment attempt for the given external customer ID.
	CountPendingInvoices(ctx context.Context, externalCustomerID string) (int, error)
	// HasOutstandingInvoices returns true if the customer has any issued invoices or draft invoices with a non-zero total.
	HasOutstandingInvoices(ctx context.Context, externalCustomerID string) (bool, error)
	// FinalizeSubscription cancels any active Orb subscriptions for the customer immediately.
	FinalizeSubscription(ctx context.Context, externalCustomerID string) error
	// DeletePaymentMethod detaches the customer's default Stripe payment method, if any.
	DeletePaymentMethod(ctx context.Context, externalCustomerID string) error
	CreateStripeCheckoutSession(ctx context.Context, externalCustomerID string) (*CheckoutSession, error)
	CreateStripePaymentMethodSession(ctx context.Context, externalCustomerID string) (*PaymentMethodSession, error)
}

type NoopBilling struct{}

func (n *NoopBilling) CreateCustomer(ctx context.Context, opts CreateCustomerOptions) (CreateCustomerResult, error) {
	return CreateCustomerResult{}, nil
}

func (n *NoopBilling) FetchCustomerWithCreditsByOrbID(ctx context.Context, customerID string) (*CustomerWithCredits, error) {
	return nil, nil
}

func (n *NoopBilling) FetchCustomerByOrbID(ctx context.Context, customerID string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) LoadCustomerCredits(_ context.Context, _ *Customer) (*CustomerWithCredits, error) {
	return nil, nil
}

func (n *NoopBilling) FetchCustomer(_ context.Context, _ string) (*Customer, error) {
	return nil, nil
}

func (n *NoopBilling) FetchCustomerWithCredits(_ context.Context, _ string) (*CustomerWithCredits, error) {
	return nil, nil
}

func (n *NoopBilling) ConfigureOrbCustomerForBankTransfers(_ context.Context, _ OrbBankTransferOptions) error {
	return nil
}

func (n *NoopBilling) UpdateOrbCustomerEmail(_ context.Context, _, _ string) (*CustomerWithCredits, error) {
	return nil, nil
}

func (n *NoopBilling) ListInvoices(_ context.Context, _ string, _ InvoiceListOptions) (*InvoicesPage, error) {
	return nil, nil
}

func (n *NoopBilling) ListInvoiceSummaries(_ context.Context, _ InvoiceSummaryListOptions) ([]InvoiceSummary, error) {
	return nil, nil
}

func (n *NoopBilling) FetchUpcomingInvoice(_ context.Context, _ string) (*UpcomingInvoice, error) {
	return nil, nil
}

func (n *NoopBilling) ListCustomersWithCreditsCreatedAfter(_ context.Context, _ time.Time) ([]*CustomerWithCredits, error) {
	return nil, nil
}

func (n *NoopBilling) FetchInvoice(_ context.Context, _ string) (*Invoice, error) {
	return nil, nil
}

func (n *NoopBilling) MarkInvoicePaid(_ context.Context, _ MarkInvoicePaidOptions) error {
	return nil
}

func (n *NoopBilling) FetchStripeCustomer(_ context.Context, _ string) (*StripeCustomer, error) {
	return nil, nil
}

func (n *NoopBilling) FindExistingStripeCustomerForBankTransfers(_ context.Context, _ FindStripeBankTransferCustomerOptions) (string, error) {
	return "", nil
}

func (n *NoopBilling) ConfigureStripeCustomerForBankTransfers(_ context.Context, _ ConfigureStripeBankTransferCustomerOptions) (StripeBankTransferResult, error) {
	return StripeBankTransferResult{}, nil
}

func (n *NoopBilling) ReconcileBankTransfer(_ context.Context, stripeCustomerID, transactionID string) (BankTransferReconciliation, error) {
	return BankTransferReconciliation{StripeCustomerID: stripeCustomerID, StripeTransactionID: transactionID}, nil
}

func (n *NoopBilling) FetchPaymentIntentPaymentMethodID(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (n *NoopBilling) FetchSetupIntentPaymentMethodID(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (n *NoopBilling) FetchStripePaymentMethodCard(_ context.Context, _ string) (*StripePaymentMethodCard, error) {
	return nil, nil
}

func (n *NoopBilling) EnsureDefaultPaymentMethod(_ context.Context, _, _ string) error {
	return nil
}

func (n *NoopBilling) SetOrbCustomerStripeChargeProvider(_ context.Context, _, _ string) error {
	return nil
}

func (n *NoopBilling) RefundPaymentIntent(_ context.Context, _ string, _ map[string]string, _ string) error {
	return nil
}

func (n *NoopBilling) HasValidDefaultPaymentMethod(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (n *NoopBilling) VoidInvoice(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) CountPendingInvoices(_ context.Context, _ string) (int, error) {
	return 0, nil
}

func (n *NoopBilling) HasOutstandingInvoices(_ context.Context, _ string) (bool, error) {
	return false, nil
}

func (n *NoopBilling) FinalizeSubscription(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) DeletePaymentMethod(_ context.Context, _ string) error {
	return nil
}

func (n *NoopBilling) CreateStripeCheckoutSession(_ context.Context, _ string) (*CheckoutSession, error) {
	return nil, nil
}

func (n *NoopBilling) CreateStripePaymentMethodSession(_ context.Context, _ string) (*PaymentMethodSession, error) {
	return nil, nil
}
